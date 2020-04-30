from enum import Enum, unique


@unique
class SparkResources(Enum):
    SMALL = ("small", "2G", "10")
    MEDIUM = ("medium", "4G", "20")
    LARGE = ("large", "8G", "40")
    CUSTOM = ("custom", "", "")

    def __init__(self, description, memory, cores):
        self.description = description
        self.memory = memory
        self.cores = cores
        self._props = {}

    @classmethod
    def from_str(cls, name):
        for resource in SparkResources:
            if resource.name == name:
                return resource
        raise ValueError('{} is not a valid SparkResource name'.format(name))

    @property
    def properties(self):
        self._props['spark.executor.memory'] = self.memory
        self._props['spark.executor.cores'] = self.cores
        self._props['spark.yarn.access.hadoopFileSystems'] = "nxcals"
        return self._props
