from kubemq.queues import *
import json


class SourceClient:
    def __init__(self, address="localhost:50000"):
        self.client = Client(address=address)

    def send_source(self, message: str) :
        send_result = self.client.send_queues_message(
            QueueMessage(
                channel="rag-sources-queue",
                body=message.encode("utf-8"),
            )
        )
        if send_result.is_error:
            print(f"message send error, error:{send_result.error}")


if __name__ == "__main__":
    client = SourceClient()
    urls = ["https://www.rottentomatoes.com/m/side_by_side_2012",
        "https://www.rottentomatoes.com/m/matrix",
        "https://www.rottentomatoes.com/m/matrix_revolutions",
        "https://www.rottentomatoes.com/m/matrix_reloaded",
        "https://www.rottentomatoes.com/m/speed_1994",
        "https://www.rottentomatoes.com/m/john_wick_chapter_4"]
    for url in urls:
        client.send_source(url)
    print("done")