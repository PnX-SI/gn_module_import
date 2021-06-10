from enum import IntEnum


class Step(IntEnum):
    Upload = 1
    Decode = 2
    FieldMapping = 3
    ContentMapping = 4
    Import = 5

    @staticmethod
    def next_step(step):
        return step + 1
