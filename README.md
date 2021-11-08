# Prototype integration of the Intake catalog within Kedro

The [Intake](https://intake.readthedocs.io/) catalog is similar to the Kedro `DataCatalog` and this repository serves as a testing ground for considering ways for the two tools to work together and provide users with a greater breadth of functionality. Eventually this repository could serve as a basis for a full fledged Kedro plugin.

## Introductions

### Intake Catalog

```yaml
metadata:
  version: 1

sources:
  reviews:
    args:
      urlpath: data/01_raw/reviews.csv
    driver: csv
    description: "A log of company reviews"
```

### After catalog created hook

```python
@hook_impl
def after_catalog_created(self, catalog: DataCatalog):

    # Could extend to make path glob pattern or be user configurable
    intake_catalog = intake.open_catalog("conf/base/intake.yml")
    intake_datasets = {
        x: IntakeDataSet(intake_catalog=intake_catalog, dataset_name=x)
        for x in list(intake_catalog)
    }

    # Add discovered datasets to Kedro catalog and prefix entries 
    for ds_name, ds_obj in intake_datasets.items():
        catalog.add(
            data_set_name=f"intake:{ds_name}", data_set=ds_obj, replace=True
        )
    
```

### Define `IntakeDataSet`

- Implementation can be found at: `extras/datasets/intake_dataset.py`
- Simple wrapper for Intake catalog object providing load functionality, save raises an error

## Results

![example](demo.png)
