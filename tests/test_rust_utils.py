import json

from zoopipe import JSONInputAdapter, JSONOutputAdapter, Pipe


def test_serde_to_py_null_conversion(tmp_path):
    input_json = tmp_path / "input.jsonl"
    output_json = tmp_path / "output.jsonl"
    input_json.write_text('{"value": null}')

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_json)),
        output_adapter=JSONOutputAdapter(str(output_json), format="jsonl"),
    )
    pipe.start()
    pipe.wait()

    with open(output_json) as f:
        result = json.loads(f.readline())
        assert result["value"] is None


def test_serde_to_py_bool_conversion(tmp_path):
    input_json = tmp_path / "input.jsonl"
    output_json = tmp_path / "output.jsonl"
    input_json.write_text('{"flag_true": true, "flag_false": false}')

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_json)),
        output_adapter=JSONOutputAdapter(str(output_json), format="jsonl"),
    )
    pipe.start()
    pipe.wait()

    with open(output_json) as f:
        result = json.loads(f.readline())
        assert result["flag_true"] is True
        assert result["flag_false"] is False


def test_serde_to_py_number_conversion(tmp_path):
    input_json = tmp_path / "input.jsonl"
    output_json = tmp_path / "output.jsonl"
    input_json.write_text('{"integer": 42, "float": 3.14, "negative": -10}')

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_json)),
        output_adapter=JSONOutputAdapter(str(output_json), format="jsonl"),
    )
    pipe.start()
    pipe.wait()

    with open(output_json) as f:
        result = json.loads(f.readline())
        assert result["integer"] == 42
        assert result["float"] == 3.14
        assert result["negative"] == -10


def test_serde_to_py_string_conversion(tmp_path):
    input_json = tmp_path / "input.jsonl"
    output_json = tmp_path / "output.jsonl"
    input_json.write_text('{"text": "hello world"}')

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_json)),
        output_adapter=JSONOutputAdapter(str(output_json), format="jsonl"),
    )
    pipe.start()
    pipe.wait()

    with open(output_json) as f:
        result = json.loads(f.readline())
        assert result["text"] == "hello world"


def test_serde_to_py_array_conversion(tmp_path):
    input_json = tmp_path / "input.jsonl"
    output_json = tmp_path / "output.jsonl"
    input_json.write_text('{"items": [1, 2, 3, "four"]}')

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_json)),
        output_adapter=JSONOutputAdapter(str(output_json), format="jsonl"),
    )
    pipe.start()
    pipe.wait()

    with open(output_json) as f:
        result = json.loads(f.readline())
        assert result["items"] == [1, 2, 3, "four"]


def test_serde_to_py_nested_object_conversion(tmp_path):
    input_json = tmp_path / "input.jsonl"
    output_json = tmp_path / "output.jsonl"
    input_json.write_text('{"outer": {"inner": {"deep": "value"}}}')

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_json)),
        output_adapter=JSONOutputAdapter(str(output_json), format="jsonl"),
    )
    pipe.start()
    pipe.wait()

    with open(output_json) as f:
        result = json.loads(f.readline())
        assert result["outer"]["inner"]["deep"] == "value"


def test_serde_to_py_complex_structure(tmp_path):
    input_json = tmp_path / "input.jsonl"
    complex_data = {
        "id": 123,
        "name": "test",
        "active": True,
        "score": 98.5,
        "tags": ["python", "rust"],
        "metadata": {"created": "2024-01-01", "author": None},
    }
    input_json.write_text(json.dumps(complex_data))
    output_json = tmp_path / "output.jsonl"

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_json)),
        output_adapter=JSONOutputAdapter(str(output_json), format="jsonl"),
    )
    pipe.start()
    pipe.wait()

    with open(output_json) as f:
        result = json.loads(f.readline())
        assert result["id"] == 123
        assert result["name"] == "test"
        assert result["active"] is True
        assert result["score"] == 98.5
        assert result["tags"] == ["python", "rust"]
        assert result["metadata"]["created"] == "2024-01-01"
        assert result["metadata"]["author"] is None


def test_py_serializable_deeply_nested(tmp_path):
    input_json = tmp_path / "input.jsonl"
    nested_data = {
        "level1": {"level2": {"level3": {"level4": [1, 2, {"key": "value"}]}}}
    }
    input_json.write_text(json.dumps(nested_data))
    output_json = tmp_path / "output.jsonl"

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_json)),
        output_adapter=JSONOutputAdapter(str(output_json), format="jsonl"),
    )
    pipe.start()
    pipe.wait()

    with open(output_json) as f:
        result = json.loads(f.readline())
        assert result["level1"]["level2"]["level3"]["level4"][2]["key"] == "value"


def test_py_serializable_mixed_types_in_array(tmp_path):
    input_json = tmp_path / "input.jsonl"
    mixed_array = {"mixed": [1, "text", True, None, 3.14, {"nested": "object"}, [1, 2]]}
    input_json.write_text(json.dumps(mixed_array))
    output_json = tmp_path / "output.jsonl"

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_json)),
        output_adapter=JSONOutputAdapter(str(output_json), format="jsonl"),
    )
    pipe.start()
    pipe.wait()

    with open(output_json) as f:
        result = json.loads(f.readline())
        assert result["mixed"][0] == 1
        assert result["mixed"][1] == "text"
        assert result["mixed"][2] is True
        assert result["mixed"][3] is None
        assert result["mixed"][4] == 3.14
        assert result["mixed"][5]["nested"] == "object"
        assert result["mixed"][6] == [1, 2]


def test_py_serializable_unicode_strings(tmp_path):
    input_json = tmp_path / "input.jsonl"
    unicode_data = {
        "english": "hello",
        "spanish": "hola",
        "chinese": "你好",
        "emoji": "🎉",
    }
    input_json.write_text(json.dumps(unicode_data, ensure_ascii=False))
    output_json = tmp_path / "output.jsonl"

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_json)),
        output_adapter=JSONOutputAdapter(str(output_json), format="jsonl"),
    )
    pipe.start()
    pipe.wait()

    with open(output_json, encoding="utf-8") as f:
        result = json.loads(f.readline())
        assert result["english"] == "hello"
        assert result["spanish"] == "hola"
        assert result["chinese"] == "你好"
        assert result["emoji"] == "🎉"


def test_py_serializable_large_numbers(tmp_path):
    input_json = tmp_path / "input.jsonl"
    large_numbers = {
        "large_int": 9223372036854775807,
        "small_int": -9223372036854775808,
        "large_float": 1.7976931348623157e308,
    }
    input_json.write_text(json.dumps(large_numbers))
    output_json = tmp_path / "output.jsonl"

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_json)),
        output_adapter=JSONOutputAdapter(str(output_json), format="jsonl"),
    )
    pipe.start()
    pipe.wait()

    with open(output_json) as f:
        result = json.loads(f.readline())
        assert result["large_int"] == 9223372036854775807
        assert result["small_int"] == -9223372036854775808
        assert abs(result["large_float"] - 1.7976931348623157e308) < 1e300


def test_py_serializable_empty_containers(tmp_path):
    input_json = tmp_path / "input.jsonl"
    empty_containers = {"empty_object": {}, "empty_array": [], "normal": "value"}
    input_json.write_text(json.dumps(empty_containers))
    output_json = tmp_path / "output.jsonl"

    pipe = Pipe(
        input_adapter=JSONInputAdapter(str(input_json)),
        output_adapter=JSONOutputAdapter(str(output_json), format="jsonl"),
    )
    pipe.start()
    pipe.wait()

    with open(output_json) as f:
        result = json.loads(f.readline())
        assert result["empty_object"] == {}
        assert result["empty_array"] == []
        assert result["normal"] == "value"
