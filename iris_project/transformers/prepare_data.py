import pandas as pd
from sklearn.model_selection import train_test_split

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
def transform(df, *args, **kwargs):
    test_size = kwargs.get('test_size', 0.2)
    train_df, test_df = train_test_split(df, test_size=test_size, random_state=42)

    train_df = train_df.copy()
    test_df = test_df.copy()
    train_df['split'] = 'train'
    test_df['split'] = 'test'

    result = pd.concat([train_df, test_df])
    print('Train rows:', len(train_df), '| Test rows:', len(test_df))
    return result


@test
def test_output(output, *args):
    assert 'split' in output.columns, 'split column missing'
    assert (output['split'] == 'train').sum() == 120, 'Expected 120 train rows'
    assert (output['split'] == 'test').sum() == 30, 'Expected 30 test rows'
