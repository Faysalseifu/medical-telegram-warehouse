import json
import pytest
from pathlib import Path
from src.scraper import _load_scrape_state, _save_scrape_state

def test_load_save_scrape_state(tmp_path):
    state_file = tmp_path / "state.json"
    
    assert _load_scrape_state(state_file) == {}
    
    state_to_save = {"channel1": 123, "channel2": 456}
    _save_scrape_state(state_file, state_to_save)
    
    assert state_file.exists()
    assert _load_scrape_state(state_file) == state_to_save
