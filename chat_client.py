from kubemq.cq import *
import json


class ChatClient:
    def __init__(self, address="localhost:50000"):
        self.client = Client(address=address)

    def send_message(self, message: str) -> str:
        response = self.client.send_query_request(QueryMessage(
            channel="rag-chat-query",
            body=message.encode('utf-8'),
            timeout_in_seconds=30
        ))
        return response.body.decode('utf-8')


if __name__ == "__main__":
    client = ChatClient()
    print("Sending first question:  Who is the director of the movie The Matrix?")
    response = client.send_message("Who is the director of the movie The Matrix?")
    print(f"Response: {response}")
    print("Sending second question:  How this director connected to Keanu Reeves?")
    response = client.send_message("How this director connected to Keanu Reeves?")
    print(f"Response: {response}")
