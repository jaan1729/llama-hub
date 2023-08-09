"""monday.com reader."""
from typing import Dict, List

from llama_index.readers.base import BaseReader
from llama_index.readers.schema.base import Document
import requests


class MondayReader(BaseReader):
    """monday.com reader. Reads board's data by a GraphQL query.

    Args:
        api_key (str): monday.com API key.
    """

    def __init__(self, api_key: str) -> None:
        """Initialize monday.com reader."""

        self.api_key = api_key
        self.api_url = "https://api.monday.com/v2"

    def _parse_item_values(self, cv) -> Dict[str, str]:
        data = {}
        data["title"] = cv["title"]
        data["value"] = cv["text"]

        return data

    def _parse_data(self, item) -> Dict[str, str]:
        data = {}
        data["id"] = item["id"]
        data["name"] = item["name"]
        data["values"] = list(map(self._parse_item_values, list(item["column_values"])))

        return data

    def _perform_request(self, board_id) -> Dict[str, str]:
        headers = {"Authorization": self.api_key}
        query = """
            query{
                boards(ids: [%d]){
                    name,
                    items{
                        id,
                        name,
                        column_values{
                            title,
                            text
                        }
                    }
                }
            } """ % (
            board_id
        )
        data = {"query": query}

        response = requests.post(url=self.api_url, json=data, headers=headers)
        return response.json()

    def load_data(self, board_id: int) -> List[Document]:
        """Load board data by board_id

        Args:
            board_id (int): monday.com board id.
        Returns:
            List[Document]: List of items as documents.
            [{id, name, values: [{title, value}]}]
        """

        json_response = self._perform_request(board_id)
        board_data = json_response["data"]["boards"][0]

        board_data["name"]
        items_array = list(board_data["items"])
        parsed_items = list(map(self._parse_data, list(items_array)))
        result = []
        for item in parsed_items:
            text = f"name: {item['name']}"
            for item_value in item["values"]:
                if item_value["value"]:
                    text += f", {item_value['title']}: {item_value['value']}"
            result.append(
                Document(
                    text=text, extra_info={"board_id": board_id, "item_id": item["id"]}
                )
            )

        return result


if __name__ == "__main__":
    reader = MondayReader("api_key")
    print(reader.load_data(12345))
"""monday.com reader."""
from typing import Dict, List
import json
from llama_index.readers.base import BaseReader
from llama_index.readers.schema.base import Document
import requests

class MondayReader(BaseReader):
    """monday.com reader. Reads board's data by a GraphQL query.

    Args:
        api_key (str): monday.com API key.
    """

    def __init__(self, api_key: str) -> None:
        """Initialize monday.com reader."""

        self.api_key = api_key
        self.api_url = "https://api.monday.com/v2"

    def _parse_item_values(self, cv) -> Dict[str, str]:
        data = {}
        data["title"] = cv["title"]
        data["value"] = cv["text"]

        return data

    def _parse_data(self, item) -> Dict[str, str]:
        data = {}
        data["id"] = item["id"]
        data["name"] = item["name"]
        data["values"] = list(map(self._parse_item_values, list(item["column_values"])))

        return data

    def _read_block(self, block):
        block_text = ""
        deltaFormat_array = json.loads(block["content"])["deltaFormat"]
        for insert in deltaFormat_array:
            text = insert["insert"]
            if "attributes" in insert:
                if "link" in insert["attributes"]:
                    text = text + " url: " + insert["attributes"]["link"]
            block_text = block_text + text + " \t"
        
        return block_text
    
    def _perform_request(self, board_id, doc_id) -> Dict[str, str]:
        headers = {"Authorization": self.api_key}
        
        if board_id:
            query = """
                query{
                    boards(ids: [%d]){
                        name,
                        items{
                            id,
                            name,
                            column_values{
                                title,
                                text
                            }
                        }
                    }
                } """ % (
                board_id
            )

        if doc_id:
            query = """
                query{
                docs (object_ids: [%d], limit: 1) {
                    name
                    object_id
                    blocks{
                            id
                            type
                            content
                        }
                    created_by {
                            id
                            name
                        }
                }
                }""" % (
                doc_id
                )

        data = {"query": query}

        response = requests.post(url=self.api_url, json=data, headers=headers)
        return response.json()

    def load_data(self, board_id: int = None, doc_id: int = None) -> List[Document]:
        """Load board data by board_id

        Args:
            board_id (int): monday.com board id.
            doc_id (int): monday.com doc id.
        Returns:
            List[Document]: List of items as documents.
            [{id, name, values: [{title, value}]}]
        """
        
        assert board_id != None or doc_id != None, "Either board_id or doc_id is required"


        json_response = self._perform_request(board_id, doc_id)
        #print(json_response["data"].keys())
        if board_id:
            data = json_response["data"]["boards"][0]
            items_array = data['items']
            parsed_items = list(map(self._parse_data, list(items_array)))
            
            result = []
            for item in parsed_items:
                text = f"name: {item['name']}"
                for item_value in item["values"]:
                    if item_value["value"]:
                        text += f", {item_value['title']}: {item_value['value']}"
                result.append(
                    Document(
                        text=text, extra_info={"board_id": board_id, "item_id": item["id"]}
                    )
                )
        elif doc_id:
            data = json_response["data"]["docs"][0]
            blocks_array = list(data["blocks"])
            lines_arr = list(map(self._read_block, blocks_array))
            doc_text = " \n".join(lines_arr)
            result = [Document(text=doc_text, extra_info={"doc_id": doc_id, "doc_name": data["name"], "created_by": data["created_by"]["name"]})]
        
        return result


if __name__ == "__main__":
    reader = MondayReader("api_key")
    print(reader.load_data(12345))
