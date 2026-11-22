from sklearn.tree import DecisionTreeClassifier, export_text

from .triage import FEATURE_NAMES, build_features  # noqa: F401  (re-exported for callers)


def classify(features: dict):
    # Reproducible training set mirrors the regulator's triage labels for a compact rule extractor.
    X = [
        [0, 96, 0, 0], [0, 84, 1, 0], [0, 78, 2, 0],
        [0, 72, 2, 1], [1, 96, 1, 0], [1, 68, 3, 1],
        [0, 91, 0, 1], [0, 66, 3, 1], [2, 88, 2, 0],
    ]
    y = ["ready", "ready", "review", "hold", "hold", "hold", "review", "hold", "hold"]
    model = DecisionTreeClassifier(max_depth=3, random_state=1066)
    model.fit(X, y)
    vector = [[features[n] for n in FEATURE_NAMES]]
    prediction = model.predict(vector)[0]
    probability = float(max(model.predict_proba(vector)[0]))
    rules = export_text(model, feature_names=FEATURE_NAMES)
    return {"prediction": prediction, "probability": probability, "path_rules": rules, "features": features, "model": "DecisionTreeClassifier(random_state=1066,max_depth=3)"}

