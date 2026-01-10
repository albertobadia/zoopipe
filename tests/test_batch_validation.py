import unittest

from pydantic import BaseModel, field_validator

from zoopipe.executor.base import BaseExecutor
from zoopipe.models.core import EntryStatus, EntryTypedDict


class FailingSchema(BaseModel):
    id: int
    val: int

    @field_validator("val")
    @classmethod
    def check_val(cls, v):
        if v < 0:
            raise ValueError("must be positive")
        return v


class TestBatchFallback(unittest.TestCase):
    def test_batch_validation_fallback(self):
        entries = []
        raw_data = [
            {"id": 1, "val": 10},
            {"id": 2, "val": 20},
            {"id": 3, "val": -1},
            {"id": 4, "val": 40},
            {"id": 5, "val": -2},
            {"id": 6, "val": 60},
        ]

        for i, data in enumerate(raw_data):
            entries.append(
                EntryTypedDict(
                    id=str(i),
                    raw_data=data,
                    validated_data=None,
                    position=i,
                    status=EntryStatus.PENDING,
                    errors=[],
                    metadata={},
                )
            )

        updated_entries = BaseExecutor._process_batch_with_fallback(
            entries, FailingSchema
        )

        self.assertEqual(updated_entries[0]["status"], EntryStatus.VALIDATED)
        self.assertEqual(updated_entries[1]["status"], EntryStatus.VALIDATED)

        self.assertEqual(updated_entries[2]["status"], EntryStatus.FAILED)
        self.assertIn("must be positive", str(updated_entries[2]["errors"]))

        self.assertEqual(updated_entries[3]["status"], EntryStatus.VALIDATED)

        self.assertEqual(updated_entries[4]["status"], EntryStatus.FAILED)

        self.assertEqual(updated_entries[5]["status"], EntryStatus.VALIDATED)

        self.assertEqual(updated_entries[0]["validated_data"]["val"], 10)


if __name__ == "__main__":
    unittest.main()
