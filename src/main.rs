use std::{io, thread};
use std::io::{Read, Write};
use std::mem::transmute;
use std::process::{Command, Stdio};
use std::sync::Arc;
use csv::StringRecord;
use neo4rs::{BoltMap, BoltNode, BoltType, ConfigBuilder, Graph, query, Row};
use serenity::{async_trait, Client};
use serenity::all::{CommandOptionType, CreateCommandOption, CreateInteractionResponse, CreateInteractionResponseFollowup, CreateInteractionResponseMessage, GuildId, Interaction, ResolvedOption, ResolvedValue};
use serenity::builder::{CreateAttachment, CreateCommand};
use serenity::model::gateway::Ready;
use serenity::prelude::*;

fn parse_row(row: &StringRecord) -> (Option<String>, Vec<String>) {
    let mut iter = row.iter().skip(3);
    let event = iter.next().unwrap();
    let people = iter.take_while(|s| !s.is_empty()).map(|s| s.to_owned()).collect::<Vec<String>>();
    let event = if event.is_empty() { None } else { Some(event.to_owned()) };
    return (event, people)
}

async fn insert_group(graph: &Graph, people: &[String]) {
    let query = query("UNWIND $names AS n1 UNWIND $names AS n2 WITH n1, n2 WHERE n1 <> n2 MERGE (p1 {name: n1}) MERGE (p2 {name: n2}) MERGE (p1)-[:MET]-(p2)").param("names", people);
    let mut rows = graph.execute(query).await.unwrap();
    while let Ok(Some(row)) = rows.next().await {
        eprintln!("{:?}", row);
    }
}

fn parse_relation(row: &Row) -> (String, String) {
    let a = row.get::<BoltNode>("n").unwrap()
        .get::<String>("name").unwrap();
    let b = row.get::<BoltNode>("m").unwrap()
        .get::<String>("name").unwrap();
    return (a, b)
}

fn parse_all_relations(row: &Row) -> Vec<String> {
    // attributes is private, there is no proper way to iterate over a row
    let map = unsafe { transmute::<&Row, &BoltMap>(row) };
    let mut out = vec![];
    for value in map.value.values() {
        if let BoltType::Node(node) = value {
            out.push(node.properties.get::<String>("name").unwrap())
        }
    }

    return out;
}

async fn export_dot(rows: &[Vec<String>]) -> String {
    let mut out = String::from("strict graph meetup_graph {\n");
    out.push_str("layout=circo\n");
    out.push_str("size=\"60,60\"\n");
    //out.push_str("edge[weight=100]");
    out.push_str("oneblock=true\n");
    for group in rows {
        let line = group.iter().map(|s| format!("\"{}\"", s.to_uppercase())).collect::<Vec<String>>().join(" -- ") + "\n";
        out.push_str(&line);
    }
    out.push('}');

    return out;
}

async fn export_graph_to_dot(graph: &Graph) -> String {
    let mut result = graph.execute(query("MATCH (n)-[]->(m) RETURN n, m")).await.unwrap();
    let mut vec = Vec::<Vec<String>>::new();
    while let Ok(Some(row)) = result.next().await {
        let (a, b) = parse_relation(&row);
        vec.push(vec![a, b]);
    }
    return export_dot(&vec).await;
}

async fn import_csv(graph: &Graph) {
    let mut result = graph.execute(query("MATCH (n) DETACH DELETE n")).await.unwrap();
    while let Some(_) = result.next().await.unwrap() {}

    let mut rdr = csv::Reader::from_reader(io::stdin());

    for result in rdr.records() {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here.
        let record = result.unwrap();
        let (_, people) = parse_row(&record);
        insert_group(&graph, &people).await;
    }
}

struct Handler;

#[async_trait]
impl EventHandler for Handler {
    async fn interaction_create(&self, ctx: Context, interaction: Interaction) {
        if let Interaction::Command(command) = interaction {
            command.create_response(&ctx.http, CreateInteractionResponse::Defer(CreateInteractionResponseMessage::new())).await.unwrap();

            let data = ctx.data.read().await;

            let response = match command.data.name.as_str() {
                "graph" => graph_command(&data.get::<BotState>().unwrap().graph, &command.data.options()).await,
                _ => Err("Command doesn't exist".to_owned()),
            };
            let followup = match response {
                Ok(bytes) => CreateInteractionResponseFollowup::new().add_file(CreateAttachment::bytes(bytes, "graph.png")),
                Err(msg) => CreateInteractionResponseFollowup::new().content(msg)
            };

            if let Err(why) = command.create_followup(&ctx.http, followup).await {
                println!("Cannot respond to slash command: {why}");
            }
        }
    }

    async fn ready(&self, ctx: Context, ready: Ready) {
        println!("{} is connected!", ready.user.name);
        let guild_id = GuildId::new(537881812249083905);

        let cmd = CreateCommand::new("graph")
            .description("query and render a subset of the graph")
            .add_option(CreateCommandOption::new(
                CommandOptionType::String,
                "query",
                "neo4j cypher query",
                ).required(true)
            )
            .add_option(CreateCommandOption::new(
                CommandOptionType::String,
                "extra_args",
                "extra args to pass to graphviz"
            ));
        guild_id.set_commands(&ctx.http, vec![cmd]).await.unwrap();
    }
}

struct BotState {
    graph: Arc<Graph>
}

impl TypeMapKey for BotState {
    type Value = BotState;
}

async fn invoke_graphviz(data: &str, extra_args: &[String]) -> Vec<u8> {
    let proc = Command::new("dot")
        .arg("-Tpng")
        .args(extra_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to start subprocess");

    let _ = thread::scope(|_| {
        proc.stdin.unwrap().write_all(data.as_bytes()).unwrap();
    });

    let mut png_data = Vec::<u8>::new();
    proc.stdout.unwrap().read_to_end(&mut png_data).unwrap();

    return png_data;
}

async fn graph_command(graph: &Graph, options: &[ResolvedOption<'_>]) -> Result<Vec<u8>, String> {
    if let ResolvedValue::String(arg) = options.iter().find(|opt| opt.name == "query").unwrap().value {
        let mut result = graph.execute(query(arg)).await.map_err(|e| format!("{:?}", e))?;
        let mut relations = Vec::<Vec<String>>::new();
        while let Ok(Some(row)) = result.next().await {
            let nodes = parse_all_relations(&row);
            relations.push(nodes);
        }
        let extra_args = options.iter()
            .find(|opt| opt.name == "extra_args").map(|opt| if let ResolvedValue::String(x) = opt.value { x } else { panic!("troll arg") } )
            .map(|s| s.split(" ").map(|s| s.to_owned()).collect::<Vec<String>>())
            .unwrap_or(vec![]);
        let dot = export_dot(&relations).await;
        let png = invoke_graphviz(&dot, &extra_args).await;

        return Ok(png);
    } else {
        return Err("missing query argument".to_owned());
    }
}

async fn discord_bot(graph: Arc<Graph>) {
    let token = std::env::var("DISCORD_TOKEN").expect("Missing discord token env variable");
    let mut client = Client::builder(token, GatewayIntents::MESSAGE_CONTENT)
        .event_handler(Handler)
        .await.unwrap();
    let mut data = client.data.write().await;
    data.insert::<BotState>(BotState{graph: graph.clone()});
    drop(data);

    if let Err(why) = client.start().await {
        println!("Client error: {why:?}");
    }
}

#[tokio::main]
async fn main() {
    let config = ConfigBuilder::default()
        .uri("127.0.0.1:7687")
        .user("neo4j")
        .password("meowmeowmeow")
        .db("meetups")
        .fetch_size(500)
        .max_connections(10)
        .build()
        .unwrap();
    let graph = Arc::new(Graph::connect(config).await.unwrap());

    let cmd = std::env::args().nth(1).expect("Expected argument");

    match cmd.to_lowercase().as_str() {
        "discord" => discord_bot(graph).await,
        "import" => import_csv(&graph).await,
        "export" => println!("{}", export_graph_to_dot(&graph).await),
        _ => panic!("invalid command")
    };
}
