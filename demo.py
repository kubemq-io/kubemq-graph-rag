from dotenv import load_dotenv

from graphrag_sdk.source import URL
from graphrag_sdk import KnowledgeGraph, Ontology
from graphrag_sdk.models.openai import OpenAiGenerativeModel
from graphrag_sdk.model_config import KnowledgeGraphModelConfig
load_dotenv()

# Import Data
urls = ["https://www.rottentomatoes.com/m/side_by_side_2012",
"https://www.rottentomatoes.com/m/matrix",
"https://www.rottentomatoes.com/m/matrix_revolutions",
"https://www.rottentomatoes.com/m/matrix_reloaded",
"https://www.rottentomatoes.com/m/speed_1994",
"https://www.rottentomatoes.com/m/john_wick_chapter_4"]

sources = [URL(url) for url in urls]

# Model
model = OpenAiGenerativeModel(model_name="gpt-4o")

# Ontology Auto-Detection
ontology = Ontology.from_sources(
    sources=sources,
    model=model,
)

# Knowledge Graph
kg = KnowledgeGraph(
    name="movies",
    model_config=KnowledgeGraphModelConfig.with_model(model),
    ontology=ontology,
)

kg.process_sources(sources)

chat = kg.chat_session()

print(chat.send_message("Who is the director of the movie The Matrix?"))
print(chat.send_message("How this director connected to Keanu Reeves?"))

