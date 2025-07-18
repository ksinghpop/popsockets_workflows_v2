import pandas as pd

def test_task(chunk:int):
    # This is a placeholder for the actual test implementation
    # You can use this function to write tests for your ETL tasks
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'value': ['a', 'b', 'c']
    })
    print(df)
    print(f"test_task called with chunk: {chunk}")
    print(f"test_task done...")