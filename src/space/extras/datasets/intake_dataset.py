from typing import Any, Dict, Optional
from kedro.io import AbstractDataSet
from intake.catalog.base import Catalog as IntakeCatalog
from intake.source.base import DataSourceBase
from kedro.io.core import DataSetError


class IntakeDataSet(AbstractDataSet):
    def __init__(
        self,
        intake_catalog: IntakeCatalog,
        dataset_name: str,
        return_object: bool = False,
    ) -> None:
        self.dataset_name = dataset_name
        self.intake_catalog = intake_catalog
        self._return_object = return_object

    
    def get_intake_dataset(self) -> DataSourceBase:
        """[summary]

        Args:
            intake_catalog (IntakeCatalog): [description]
            dataset_name (str): [description]

        Raises:
            DataSetError: [description]

        Returns:
            DataSourceBase: [description]
        """
        retrieved_attribute = getattr(self.intake_catalog, self.dataset_name, None)
        if isinstance(retrieved_attribute, DataSourceBase):
            return retrieved_attribute
        else:
            raise DataSetError(
                f"Unable to retrieve intake catalog entry from available "
                f"options: {list(self.intake_catalog)}. Please ensure that "
                f"there isn't a conflict with default attributes of the "
                f"`intake.catalog.base.Catalog` object."
            )

    def _load(self) -> Any:
        """[summary]

        Returns:
            Any: [description]
        """
        retrieved_attribute = self.get_intake_dataset()
        if self._return_object:
            return retrieved_attribute
        else:
            return retrieved_attribute.read()

    def _save(self, _: Any) -> None:
        """[summary]

        Args:
            _ (Any): [description]

        Raises:
            DataSetError: [description]
        """
        raise DataSetError(
            "Intake catalog entries do not support saving, "
            "please create a Kedro DataCatalog entry to save this object"
        )

    def _exists(self) -> bool:
        """[summary]

        Returns:
            bool: [description]
        """
        dataset = self.get_intake_dataset()
        filesystem = dataset.catalog_object.filesystem
        return filesystem.exists(dataset.urlpath)

    def _describe(self) -> Dict[str, Any]:
        """[summary]

        Returns:
            Dict[str, Any]: [description]
        """
        return self.get_intake_dataset()
