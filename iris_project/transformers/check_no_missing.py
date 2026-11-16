import pandas as pd

if 'transformer' not in globals():
    try:
        from mage_ai.data_preparation.decorators import transformer
    except ImportError:
        def transformer(fn):
            return fn

if 'test' not in globals():
    try:
        from mage_ai.data_preparation.decorators import test
    except ImportError:
        def test(fn):
            return fn


@transformer
def check_data(df, *args, **kwargs):
    """
    Validate that the raw dataset has:
    1. No missing/null values in any feature column.
    2. Exactly 3 unique target classes.
    """
    missing_count = df.isnull().sum().sum()
    unique_classes = df['target'].nunique()
    print(f'Validation: Missing values = {missing_count}, Unique classes = {unique_classes}')
    return df


@test
def test_data_quality(output, *args):
    assert output.isnull().sum().sum() == 0, 'Dataset contains missing values!'
    assert output['target'].nunique() == 3, f'Expected 3 classes, got {output["target"].nunique()}'
