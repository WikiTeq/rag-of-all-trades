import unittest
from unittest.mock import MagicMock, patch

import requests

from utils.graphql import GraphQLError, graphql_request


def _mock_response(status_code: int, json_data: dict) -> MagicMock:
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
    else:
        resp.raise_for_status.return_value = None
    return resp


class TestGraphqlRequest(unittest.TestCase):
    @patch("utils.graphql.requests.post")
    def test_success_returns_data(self, mock_post):
        mock_post.return_value = _mock_response(200, {"data": {"users": [{"id": 1}]}})

        result = graphql_request("http://api/graphql", "{ users { id } }", {}, {})

        self.assertEqual(result, {"users": [{"id": 1}]})

    @patch("utils.graphql.requests.post")
    def test_graphql_errors_raises(self, mock_post):
        mock_post.return_value = _mock_response(200, {"errors": [{"message": "Not found"}]})

        with self.assertRaises(GraphQLError) as ctx:
            graphql_request("http://api/graphql", "{ x }", {}, {})

        self.assertIn("Not found", str(ctx.exception))

    @patch("utils.graphql.requests.post")
    def test_http_error_raises(self, mock_post):
        mock_post.return_value = _mock_response(401, {})

        with self.assertRaises(requests.HTTPError):
            graphql_request("http://api/graphql", "{ x }", {}, {})

    @patch("utils.graphql.requests.post")
    def test_passes_variables_and_headers(self, mock_post):
        mock_post.return_value = _mock_response(200, {"data": {}})
        headers = {"Authorization": "Bearer token"}
        variables = {"id": 42}

        graphql_request("http://api/graphql", "query($id: ID!) { x(id: $id) }", variables, headers)

        call_kwargs = mock_post.call_args[1]
        self.assertEqual(call_kwargs["headers"], headers)
        self.assertEqual(call_kwargs["json"]["variables"], variables)

    @patch("utils.graphql.requests.post")
    def test_empty_data_returns_empty_dict(self, mock_post):
        mock_post.return_value = _mock_response(200, {})

        result = graphql_request("http://api/graphql", "{ x }", {}, {})

        self.assertEqual(result, {})

    @patch("utils.graphql.requests.post")
    def test_optional_variables_and_headers(self, mock_post):
        mock_post.return_value = _mock_response(200, {"data": {}})

        graphql_request("http://api/graphql", "{ x }")

        call_kwargs = mock_post.call_args[1]
        self.assertEqual(call_kwargs["json"]["variables"], {})
        self.assertEqual(call_kwargs["headers"], {})

    @patch("utils.graphql.requests.post")
    def test_non_json_response_raises_graphql_error(self, mock_post):
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        resp.json.side_effect = ValueError("no json")
        resp.text = "Internal Server Error"
        mock_post.return_value = resp

        with self.assertRaises(GraphQLError):
            graphql_request("http://api/graphql", "{ x }")


if __name__ == "__main__":
    unittest.main()
