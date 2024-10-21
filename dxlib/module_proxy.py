from logging import Logger
from typing import TypeVar, Generic, Optional, Type, Any
import importlib

T = TypeVar('T')


class ModuleProxy(Generic[T]):
    """
    A proxy class to lazy load a module and its objects.
    Useful when underlying module needs to be exposed but requires heavy or independent dependencies.
    """
    def __init__(self, module_name: str, obj_name: Optional[str] = None, logger: Logger = None):
        """
        Args:
            module_name: The module name to load.
            obj_name (Optional): The object name to load from the module.
        """
        self._module_name = module_name
        self._obj_name = obj_name
        self._module = None
        self._obj = None
        self._logger = logger or Logger("ModuleProxy", level="DEBUG")

    def _load_module(self):
        if self._module is None:
            self._logger.info(f"Lazy loading module: {self._module_name}")
            self._module = importlib.import_module(self._module_name)
            if self._obj_name:
                self._obj = getattr(self._module, self._obj_name)

    def __getattr__(self, item: str) -> Any:
        self._load_module()
        if self._obj_name:
            return getattr(self._obj, item)
        return getattr(self._module, item)

    def __getitem__(self, obj_type: Type[T]):
        """
        Get an object from the module by type.

        Args:
            obj_type: The type of object to get from the module.

        Returns:
            A proxy function to get the typed object from the module.

        Example:
            >>> from dxlib.module_proxy import ModuleProxy
            >>> from dxlib.interfaces import Interface
            >>> investing_com = ModuleProxy("dxlib.interfaces.external.investing_com")
            >>> api = investing_com[Interface]("InvestingCom")
        """
        def get_obj_from_module(obj_name: str) -> Type[T]:
            self._load_module()
            return getattr(self._module, obj_name)

        return get_obj_from_module


