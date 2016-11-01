## Simple ETL template
Nice to have template build on top on Luigi, created just for fun but it can be easily
extended and refactored. It provides a sample way of splitting E, T and L steps with
separate tasks.

How to start using:

```
pip install luigi
```

```
PYTHONPATH='' luigi --module etl ProviderRefinery
```


