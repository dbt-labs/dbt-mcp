from abc import ABC, abstractmethod


class ConfigProvider[ConfigType](ABC):
    @abstractmethod
    async def get_config(self) -> ConfigType: ...
