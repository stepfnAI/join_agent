from sfn_blueprint import MODEL_CONFIG

MODEL_CONFIG["join_suggestions_generator"] = {
    "model": "gpt-4o",
    "temperature": 0.3,
    "max_tokens": 1000,
    "n": 1,
    "stop": None
}