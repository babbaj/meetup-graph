use std::{io, thread};
use std::io::{Read, Write};
use std::mem::transmute;
use std::process::{Command, Stdio};
use std::sync::Arc;
use csv::StringRecord;
use neo4rs::{BoltMap, BoltNode, BoltType, ConfigBuilder, Graph, query, Row};
use serenity::{async_trait, Client};
use serenity::all::{CommandInteraction, CommandOptionType, CreateCommandOption, CreateInteractionResponse, CreateInteractionResponseFollowup, CreateInteractionResponseMessage, GuildId, Interaction, ResolvedOption, ResolvedValue};
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
    let query = query("UNWIND $names AS n1 UNWIND $names AS n2 WITH n1, n2 WHERE n1 <> n2 MERGE (p1 {name: toLower(n1)}) MERGE (p2 {name: toLower(n2)}) MERGE (p1)-[:MET]-(p2)").param("names", people);
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

fn row_attrs(row: &Row) -> &BoltMap {
    // attributes is private, there is no proper way to iterate over a row
    let map = unsafe { transmute::<&Row, &BoltMap>(row) };
    return map;
}

fn parse_all_relations(row: &Row) -> Vec<String> {
    let map = row_attrs(row);
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

async fn defer(ctx: &Context, command: &CommandInteraction) {
    command.create_response(&ctx.http, CreateInteractionResponse::Defer(CreateInteractionResponseMessage::new())).await.unwrap();
}

async fn render_graph_followup(result: Result<Vec<u8>, String>, ctx: &Context, command: &CommandInteraction) {
    let followup = match result {
        Ok(bytes) => CreateInteractionResponseFollowup::new().add_file(CreateAttachment::bytes(bytes, "graph.png")),
        Err(msg) => CreateInteractionResponseFollowup::new().content(msg)
    };
    if let Err(why) = command.create_followup(&ctx.http, followup).await {
        println!("Cannot respond to slash command: {why}");
    }
}

struct Handler;

#[async_trait]
impl EventHandler for Handler {
    async fn interaction_create(&self, ctx: Context, interaction: Interaction) {
        if let Interaction::Command(command) = interaction {
            let data = ctx.data.read().await;

            let graph = &data.get::<BotState>().unwrap().graph;
            match command.data.name.as_str() {
                "graph" => {
                    defer(&ctx, &command).await;
                    render_graph_followup(graph_command(graph, &command.data.options()).await, &ctx, &command).await;
                },
                "graphquery" => {
                    defer(&ctx, &command).await;
                    render_graph_followup(graph_query_command(graph, &command.data.options()).await, &ctx, &command).await;
                }
                "query" => {
                    defer(&ctx, &command).await;
                    let text = query_command(&command.data.options()).await;
                    let response = if text.len() > 1990 {
                        CreateInteractionResponseFollowup::new().add_file(CreateAttachment::bytes(text.as_bytes(), "response.txt"))
                    } else {
                        CreateInteractionResponseFollowup::new().content(format!("```\n{}```", text))
                    };

                    if let Err(why) = command.create_followup(&ctx.http, response).await {
                        println!("Cannot respond to slash command: {why}");
                    }
                }
                _ => command.create_response(&ctx.http, CreateInteractionResponse::Message(CreateInteractionResponseMessage::new().content("Command doesn't exist"))).await.unwrap()
            };
        }
    }

    async fn ready(&self, ctx: Context, ready: Ready) {
        println!("{} is connected!", ready.user.name);
        let guild_id = GuildId::new(343851086026637313);

        let extra_args = CreateCommandOption::new(
            CommandOptionType::String,
            "extra_args",
            "extra args to pass to graphviz"
        );
        let graph_cmd = CreateCommand::new("graph")
            .description("Render only the connections to a specific person")
            .add_option(CreateCommandOption::new(
                CommandOptionType::String,
                "who",
                "the person",
            ).required(true))
            .add_option(extra_args.clone());
        let graph_query_cmd = CreateCommand::new("graphquery")
            .description("query and render a subset of the graph")
            .add_option(CreateCommandOption::new(
                CommandOptionType::String,
                "query",
                "neo4j cypher query",
            ).required(true)
            )
            .add_option(extra_args.clone());
        let query_cmd = CreateCommand::new("query")
            .description("Query the neo4j database")
            .add_option(CreateCommandOption::new(
                CommandOptionType::String,
                "query",
                "neo4j cypher query",
            ).required(true));
        guild_id.set_commands(&ctx.http, vec![graph_cmd, graph_query_cmd, query_cmd]).await.unwrap();
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

async fn generate_graph(graph: &Graph, query: neo4rs::Query, options: &[ResolvedOption<'_>]) -> Result<Vec<u8>, String> {
    let mut result = graph.execute(query).await.map_err(|e| format!("{:?}", e))?;
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
}

async fn graph_command(graph: &Graph, options: &[ResolvedOption<'_>]) -> Result<Vec<u8>, String> {
    if let ResolvedValue::String(who) = options.iter().find(|opt| opt.name == "who").unwrap().value {
        let query = query("MATCH (n {name: $name})-[]->(m) RETURN n, m").param("name", who.to_lowercase());
        return generate_graph(graph, query, options).await;
    } else {
        return Err("missing argument".to_owned());
    }
}

async fn graph_query_command(graph: &Graph, options: &[ResolvedOption<'_>]) -> Result<Vec<u8>, String> {
    if let ResolvedValue::String(q) = options.iter().find(|opt| opt.name == "query").unwrap().value {
        return generate_graph(graph, query(q), options).await;
    } else {
        return Err("missing argument".to_owned());
    }
}

async fn query_command(options: &[ResolvedOption<'_>]) -> String {
    if let ResolvedValue::String(query) = options.iter().find(|opt| opt.name == "query").unwrap().value {
        let proc = Command::new("cypher-shell")
            .arg("--format=verbose")
            .arg("-u=neo4j")
            .arg("-p=meowmeowmeow")
            .arg("-d=meetups")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start cypher-shell subprocess");

        let _ = thread::scope(|_| {
            proc.stdin.unwrap().write_all(query.as_bytes()).unwrap();
        });

        let mut out = Vec::<u8>::new();
        proc.stdout.unwrap().read_to_end(&mut out).unwrap();
        proc.stderr.unwrap().read_to_end(&mut out).unwrap();
        return String::from_utf8(out).unwrap();
    }
    return "Missing argument".to_owned();
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
