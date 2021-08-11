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


def get_demog_coding_plans(pipeline_name):
    return [
        CodingPlan(
            dataset_name="gender",
            raw_field="gender_raw",
            time_field="gender_time",
            coda_filename="Kenya_Pool_gender.json",
            coding_configurations=[
                CodingConfiguration(
                    coding_mode=CodingModes.SINGLE,
                    code_scheme=CodeSchemes.GENDER,
                    cleaner=swahili.DemographicCleaner.clean_gender,
                    coded_field="gender_coded",
                    analysis_file_key="gender",
                    fold_strategy=FoldStrategies.assert_label_ids_equal
                )
            ],
            ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("gender"),
            raw_field_fold_strategy=FoldStrategies.assert_equal
        ),

        CodingPlan(dataset_name="age",
                   raw_field="age_raw",
                   time_field="age_time",
                   coda_filename="Kenya_Pool_age.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.AGE,
                           cleaner=clean_age_with_range_filter,
                           coded_field="age_coded",
                           analysis_file_key="age",
                           include_in_theme_distribution=False,
                           fold_strategy=FoldStrategies.assert_label_ids_equal
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.AGE_CATEGORY,
                           coded_field="age_category_coded",
                           analysis_file_key="age_category",
                           fold_strategy=FoldStrategies.assert_label_ids_equal
                       )
                   ],
                   code_imputation_function=code_imputation_functions.impute_age_category,
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("age"),
                   raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(dataset_name="location",
                   raw_field="location_raw",
                   time_field="location_time",
                   coda_filename="Kenya_Pool_location.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.KENYA_COUNTY,
                           coded_field="county_coded",
                           analysis_file_key="county",
                           fold_strategy=FoldStrategies.assert_label_ids_equal
                       ),
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.KENYA_CONSTITUENCY,
                           coded_field="constituency_coded",
                           analysis_file_key="constituency",
                           fold_strategy=FoldStrategies.assert_label_ids_equal
                       )
                   ],
                   code_imputation_function=code_imputation_functions.impute_kenya_location_codes,
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("location"),
                   raw_field_fold_strategy=FoldStrategies.assert_equal),

        CodingPlan(dataset_name="disabled",
                   raw_field="disabled_raw",
                   time_field="disabled_time",
                   coda_filename="Kenya_Pool_disabled.json",
                   coding_configurations=[
                       CodingConfiguration(
                           coding_mode=CodingModes.SINGLE,
                           code_scheme=CodeSchemes.DISABLED,
                           coded_field="disabled_coded",
                           analysis_file_key="disabled",
                           fold_strategy=FoldStrategies.assert_label_ids_equal
                       )
                   ],
                   ws_code=CodeSchemes.WS_CORRECT_DATASET.get_code_with_match_value("disabled"),
                   raw_field_fold_strategy=FoldStrategies.assert_equal)
    ]


def get_ws_correct_dataset_scheme(pipeline_name):
    return CodeSchemes.WS_CORRECT_DATASET


def get_follow_up_coding_plans(pipeline_name):
    return []
