import pytest

from pop_predict.predict import populationPredictor

pop = populationPredictor()

def test_predict():
    result = pop.predict(2020)
    assert result == 3433000

    result = pop.predict(2050)
    assert result == 3795162
