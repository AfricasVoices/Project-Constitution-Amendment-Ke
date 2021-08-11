import json

from core_data_modules.data_models import CodeScheme


def _open_scheme(filename):
    with open(f"code_schemes/{filename}", "r") as f:
        firebase_map = json.load(f)
        return CodeScheme.from_firebase_map(firebase_map)


class CodeSchemes(object):
    APPOINTMENT_VIEWS = _open_scheme("appointment_views.json")
    APPOINTMENT_BENEFITS = _open_scheme("appointment_benefits.json")
    APPOINTMENT_CHALLENGES = _open_scheme("appointment_challenges.json")
    OTHER_MESSAGES = _open_scheme("other_messages.json")
    
    WS_CORRECT_DATASET = _open_scheme("ws_correct_dataset.json")
