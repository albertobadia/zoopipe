import pytest
import time
import tempfile
import os
from pathlib import Path
from pydantic import BaseModel
from zoopipe import Pipe, CSVInputAdapter, CSVOutputAdapter


class SimpleSchema(BaseModel):
    name: str
    value: int


def test_pipe_graceful_shutdown_with_context_manager():
    """Test that pipe shuts down gracefully when using context manager"""
    with tempfile.TemporaryDirectory() as tmpdir:
        input_file = Path(tmpdir) / "input.csv"
        output_file = Path(tmpdir) / "output.csv"
        
        input_file.write_text("name,value\ntest,123\n")
        
        with Pipe(
            input_adapter=CSVInputAdapter(str(input_file)),
            output_adapter=CSVOutputAdapter(str(output_file)),
            schema_model=SimpleSchema,
        ) as pipe:
            time.sleep(0.5)
        
        assert output_file.exists()
        output_content = output_file.read_text()
        assert "test" in output_content


def test_pipe_thread_finishes_on_context_exit():
    """Test that pipeline thread finishes when context exits"""
    with tempfile.TemporaryDirectory() as tmpdir:
        input_file = Path(tmpdir) / "input.csv"
        output_file = Path(tmpdir) / "output.csv"
        
        input_file.write_text("name,value\ntest1,1\ntest2,2\n")
        
        pipe = Pipe(
            input_adapter=CSVInputAdapter(str(input_file)),
            output_adapter=CSVOutputAdapter(str(output_file)),
            schema_model=SimpleSchema,
        )
        
        with pipe:
            time.sleep(0.3)
        
        assert not pipe._thread.is_alive(), "Thread should not be alive after context exit"
        assert output_file.exists()


def test_pipe_shutdown_method():
    """Test explicit shutdown method works correctly"""
    with tempfile.TemporaryDirectory() as tmpdir:
        input_file = Path(tmpdir) / "input.csv"
        output_file = Path(tmpdir) / "output.csv"
        
        input_file.write_text("name,value\ntest,999\n")
        
        pipe = Pipe(
            input_adapter=CSVInputAdapter(str(input_file)),
            output_adapter=CSVOutputAdapter(str(output_file)),
            schema_model=SimpleSchema,
        )
        
        pipe.start()
        time.sleep(0.2)
        pipe.shutdown(timeout=2.0)
        
        assert pipe.report.is_finished or not pipe._thread.is_alive()


def test_pipe_wait_completes():
    """Test that wait() works with non-daemon threads"""
    with tempfile.TemporaryDirectory() as tmpdir:
        input_file = Path(tmpdir) / "input.csv"
        output_file = Path(tmpdir) / "output.csv"
        
        input_file.write_text("name,value\ntest,123\n")
        
        pipe = Pipe(
            input_adapter=CSVInputAdapter(str(input_file)),
            output_adapter=CSVOutputAdapter(str(output_file)),
            schema_model=SimpleSchema,
        )
        
        pipe.start()
        completed = pipe.wait(timeout=5.0)
        
        assert completed, "Pipeline should complete within timeout"
        assert pipe.report.is_finished
        assert not pipe._thread.is_alive()
