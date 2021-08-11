from functools import partial

from core_data_modules.cleaners import swahili, Codes
from core_data_modules.data_models import CodeScheme
from core_data_modules.traced_data.util.fold_traced_data import FoldStrategies

from configurations import code_imputation_functions
from configurations.code_schemes import CodeSchemes
from src.lib.configuration_objects import CodingConfiguration, CodingModes, CodingPlan


def clean_age_with_range_filter(text):
    """
    Cleans age from the given `text`, setting to NC if the cleaned age is not in the range 10 <= age < 100.
    """
    age = swahili.DemographicCleaner.clean_age(text)
    if type(age) == int and 10 <= age < 100:
        return str(age)
        # TODO: Once the cleaners are updated to not return Codes.NOT_CODED, this should be updated to still return
        #       NC in the case where age is an int but is out of range
    else:
        return Codes.NOT_CODED


def get_rqa_coding_plans(pipeline_name):
    return [
        CodingPlan(
            raw_field="appointment_views_raw",
            dataset_name="appointment_views",
            time_field="sent_on",
            run_id_field="appointment_views_run_id",
            coda_filename="KE-Constitution-Review_appointment_views.json",
            icr_filename="appointment_views.csv",
            coding_configurations=[
                CodingConfiguration(
                    coding_mode=CodingModes.MULTIPLE,
                    code_scheme=CodeSchemes.APPOINTMENT_VIEWS,
                    coded_field="appointment_views_coded",
                    analysis_file_key="appointment_views",
                    fold_strategy=partial(FoldStrategies.list_of_labels, CodeSchemes.APPOINTMENT_VIEWS)
                )
            ],
            raw_field_fold_strategy=FoldStrategies.concatenate
        ),
        CodingPlan(
            raw_field="appointment_benefit_raw",
            dataset_name="appointment_benefit",
            time_field="sent_on",
            run_id_field="appointment_benefit_run_id",
            coda_filename="KE-Constitution-Review_appointment_benefit.json",
            icr_filename="appointment_benefit.csv",
            coding_configurations=[
                CodingConfiguration(
                    coding_mode=CodingModes.MULTIPLE,
                    code_scheme=CodeSchemes.APPOINTMENT_BENEFIT,
                    coded_field="appointment_benefit_coded",
                    analysis_file_key="appointment_benefit",
                    fold_strategy=partial(FoldStrategies.list_of_labels, CodeSchemes.APPOINTMENT_BENEFIT)
                )
            ],
            raw_field_fold_strategy=FoldStrategies.concatenate
        ),
        CodingPlan(
            raw_field="appointment_challenges_raw",
            dataset_name="appointment_challenges",
            time_field="sent_on",
            run_id_field="appointment_challenges_run_id",
            coda_filename="KE-Constitution-Review_appointment_challenges.json",
            icr_filename="appointment_challenges.csv",
            coding_configurations=[
                CodingConfiguration(
                    coding_mode=CodingModes.MULTIPLE,
                    code_scheme=CodeSchemes.APPOINTMENT_CHALLENGES,
                    coded_field="appointment_challenges_coded",
                    analysis_file_key="appointment_challenges",
                    fold_strategy=partial(FoldStrategies.list_of_labels, CodeSchemes.APPOINTMENT_CHALLENGES)
                )
            ],
            raw_field_fold_strategy=FoldStrategies.concatenate
        ),
        CodingPlan(
            raw_field="other_messages_raw",
            dataset_name="other_messages",
            time_field="sent_on",
            run_id_field="other_messages_run_id",
            coda_filename="KE-Constitution-Review_other_messages.json",
            icr_filename="other_messages.csv",
            coding_configurations=[
                CodingConfiguration(
                    coding_mode=CodingModes.MULTIPLE,
                    code_scheme=CodeSchemes.OTHER_MESSAGES,
                    coded_field="other_messages_coded",
                    analysis_file_key="other_messages",
                    fold_strategy=partial(FoldStrategies.list_of_labels, CodeSchemes.OTHER_MESSAGES)
                )
            ],
            raw_field_fold_strategy=FoldStrategies.concatenate
        )
    ]


def get_ws_correct_dataset_scheme(pipeline_name):
    return CodeSchemes.WS_CORRECT_DATASET


def get_follow_up_coding_plans(pipeline_name):
    return []
