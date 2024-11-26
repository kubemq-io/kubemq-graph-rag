import json
import threading
from typing import List

from dotenv import load_dotenv
load_dotenv()
import time
from kubemq.common import CancellationToken
from kubemq.cq import Client as CQClient, QueryMessageReceived, QueryResponseMessage, QueriesSubscription
from kubemq.queues import Client as QueuesClient
from graphrag_sdk.models.openai import OpenAiGenerativeModel
from graphrag_sdk.model_config import KnowledgeGraphModelConfig
from graphrag_sdk import KnowledgeGraph, Ontology
from graphrag_sdk.source import URL

class RAGServer:
    def __init__(self):
        self.cq_client = CQClient(address="localhost:50000")
        self.queues_client = QueuesClient(address="localhost:50000")
        model = OpenAiGenerativeModel(model_name="gpt-4o")
        with open("ontology.json", "r") as f:
            ontology = json.load(f)
        ontology = Ontology.from_json(ontology)
        self.kg = KnowledgeGraph(
            name="movies",
            model_config=KnowledgeGraphModelConfig.with_model(model),
            ontology=ontology)
        self.chat = self.kg.chat_session()
        self.shutdown_event = threading.Event()
        self.threads: List[threading.Thread] = []



    def handle_chat(self, request: QueryMessageReceived):
        try:
            message = request.body.decode('utf-8')
            print(f"Received chat message: {message}")
            result= self.chat.send_message(message)
            answer = result.get("response","No answer")
            print(f"Chat response: {answer}")
            response = QueryResponseMessage(
                query_received=request,
                is_executed=True,
                body=answer.encode('utf-8')
            )
            self.cq_client.send_response_message(response)
        except Exception as e:
            print(f"Error processing chat message: {str(e)}")
            self.cq_client.send_response_message(QueryResponseMessage(
                query_received=request,
                is_executed=False,
                error=str(e)
            ))

    def pull_from_queue(self):
        while not self.shutdown_event.is_set():
            try:
                result = self.queues_client.pull("rag-sources-queue", 10, 1)
                if result.is_error:
                    print(f"Error pulling message from queue: {result.error}")
                    continue
                sources = []
                for message in result.messages:
                    source = message.body.decode('utf-8')
                    print(f"Received source: {source}, adding to knowledge graph")
                    sources.append(URL(message.body.decode('utf-8')))
                if sources:
                    self.kg.process_sources(sources)
            except Exception as e:
                if not self.shutdown_event.is_set():  # Only log if not shutting down
                    print(f"Error processing sources: {str(e)}")

    def subscribe_to_chat_queries(self):
        def on_error(err: str):
            if not self.shutdown_event.is_set():  # Only log if not shutting down
                print(f"Error: {err}")

        cancellation_token = CancellationToken()

        try:
            self.cq_client.subscribe_to_queries(
                subscription=QueriesSubscription(
                    channel="rag-chat-query",
                    on_receive_query_callback=self.handle_chat,
                    on_error_callback=on_error,
                ),
                cancel=cancellation_token
            )

            # Wait for shutdown signal
            while not self.shutdown_event.is_set():
                time.sleep(0.1)

            # Cancel subscription when shutdown is requested
            cancellation_token.cancel()

        except Exception as e:
            if not self.shutdown_event.is_set():
                print(f"Error in subscription thread: {str(e)}")
    def run(self):

        chat_thread = threading.Thread(target=self.subscribe_to_chat_queries)
        queue_thread = threading.Thread(target=self.pull_from_queue)

        self.threads.extend([chat_thread, queue_thread])

        for thread in self.threads:
            thread.daemon = True  # Make threads daemon so they exit when main thread exits
            thread.start()

        print("RAG server started")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down gracefully...")
            self.shutdown()
            self.cq_client.close()
            self.queues_client.close()

    def shutdown(self):

        print("Initiating shutdown sequence...")
        self.shutdown_event.set()  # Signal all threads to stop

        for thread in self.threads:
            thread.join(timeout=5.0)  # Wait up to 5 seconds for each thread
            if thread.is_alive():
                print(f"Warning: Thread {thread.name} did not shutdown cleanly")

        print("Shutdown complete")
if __name__ == "__main__":
    rag_server = RAGServer()
    rag_server.run()