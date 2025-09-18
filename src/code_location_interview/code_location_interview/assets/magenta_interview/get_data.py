import logging
import random
import sys

import numpy as np
import pandas as pd
from dagster import AssetIn, AssetOut, asset, get_dagster_logger, multi_asset, AutomationCondition, file_relative_path

log_fmt = "[%(asctime)s] %(message)s"
log_datefmt = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(stream=sys.stdout, format=log_fmt, datefmt=log_datefmt, level=logging.INFO)
logger = get_dagster_logger(__name__)


group_name = "get_data"


@multi_asset(
    group_name=group_name,
    outs={
        "rating_account_id": AssetOut(
            automation_condition=AutomationCondition.on_cron("0 1 * * 1")
        ),
        "unique_customer_ids": AssetOut(
            automation_condition=AutomationCondition.on_cron("0 1 * * 1")
        ),
        "core_data": AssetOut(
            automation_condition=AutomationCondition.on_cron("0 1 * * 1"),
        ),
    },
)
def core_data():
    num_rows = 100000
    num_unique_customers = int(num_rows * 0.85)
    rating_account_id = np.random.choice(np.arange(100000, 1000000), size=num_rows, replace=False)
    unique_customer_ids = list(
        set([f"{random.randint(1, 5)}.{random.randint(100000, 999999)}" for _ in range(num_unique_customers)])
    )

    # Assign 'customer_id's to 'rating_account_id's, allowing repeats
    customer_id = np.random.choice(unique_customer_ids, size=num_rows, replace=True)

    # Generate 'age' (integer between 18 and 100, peak between 35 and 55, few values >= 75)
    logger.info("Age")
    age_distribution = np.concatenate(
        [
            np.random.normal(45, 7, int(num_rows * 0.80)),
            np.random.randint(18, 35, int(num_rows * 0.15)),
            np.random.randint(75, 101, int(num_rows * 0.05)),
        ]
    )
    age = np.clip(age_distribution, 18, 100).astype(int)

    # Generate 'contract_lifetime_days' (integer between 7 and 5*365, few cases higher than 3*365)
    logger.info("contract_lifetime_days")
    contract_lifetime_days = np.concatenate(
        [
            np.random.randint(7, 3 * 365 + 1, int(num_rows * 0.75)),
            np.random.randint(3 * 365 + 1, 5 * 365 + 1, int(num_rows * 0.25)),
        ]
    )

    # Generate 'remaining_binding_days' (integer between -2*365 and 2*365, abs value < contract_lifetime_days)
    logger.info("remaining_binding_days")
    remaining_binding_days = []
    for cld in contract_lifetime_days:
        max_remaining = min(2 * 365, cld - 1)  # Ensure abs(remaining_binding_days) < contract_lifetime_days
        rbd = np.random.randint(-2 * 365, max_remaining + 1)
        while abs(rbd) >= cld:
            rbd = np.random.randint(-2 * 365, max_remaining + 1)
        remaining_binding_days.append(rbd)
    remaining_binding_days = np.array(remaining_binding_days)

    # Generate 'has_special_offer' (binary 1 or 0, 30% are 1)
    logger.info("has_special_offer")
    has_special_offer = np.random.choice([0, 1], size=num_rows, p=[0.7, 0.3])

    # Generate 'is_magenta1_customer' (binary 1 or 0, 30% are 1)
    is_magenta1_customer = np.random.choice([0, 1], size=num_rows, p=[0.7, 0.3])

    # Generate 'available_gb' (integer, can be 0, 10, 20, 30, 40, 50, null)
    logger.info("available_gb")
    available_gb_options = [0, 10, 20, 30, 40, 50, None]
    available_gb = [random.choice(available_gb_options) for _ in range(num_rows)]
    available_gb = np.array(available_gb)

    # Generate 'gross_mrc' (float between 5 and 70, around 30 different values)
    logger.info("gross_mrc")
    gross_mrc_values = np.round(np.linspace(5, 70, num=50), 2)
    gross_mrc = np.random.choice(gross_mrc_values, size=num_rows)

    # Generate 'smartphone_brand' (categorical)
    logger.info("smartphone_brand")
    smartphone_brand_options = ["iPhone", "Samsung", "Huawei", "Xiaomi", "OnePlus"]
    smartphone_brand = np.random.choice(smartphone_brand_options, size=num_rows, p=[0.4, 0.35, 0.2, 0.025, 0.025])

    # Create DataFrame
    logger.info("Create df")
    core_data = pd.DataFrame(
        {
            "rating_account_id": rating_account_id,
            "customer_id": customer_id,
            "age": age,
            "contract_lifetime_days": contract_lifetime_days,
            "remaining_binding_days": remaining_binding_days,
            "has_special_offer": has_special_offer,
            "is_magenta1_customer": is_magenta1_customer,
            "available_gb": available_gb,
            "gross_mrc": gross_mrc,
            "smartphone_brand": smartphone_brand,
        }
    )

    return (
        pd.DataFrame({"rating_account_id": rating_account_id}),
        pd.DataFrame({"customer_id": unique_customer_ids}),
        core_data,
    )


@asset(
    group_name=group_name,
    automation_condition=AutomationCondition.eager()
)
def label(core_data):
    # Generate 'has_churned' with correlations and adjusted mean churn rate
    churn_prob = (
        0.1 * (core_data.age > 45)
        + 0.15  # Older customers have a higher probability to churn
        * ((core_data.contract_lifetime_days >= 650) & (core_data.contract_lifetime_days <= 850))
        + 0.2
        * (
            (core_data.remaining_binding_days >= 60) & (core_data.remaining_binding_days <= 120)
        )  # Contract lifetime around 2 years
        + -0.1 * (core_data.has_special_offer == 0)  # Remaining binding days between 60 and 120
        + -0.1 * (core_data.is_magenta1_customer == 0)  # Special offer reduces churn probability
        + 0.05 * (np.array(core_data.available_gb, dtype=float) > 30)  # Magenta1 customer reduces churn probability
        + 0.1  # Higher available GB increases churn probability
        * (core_data.gross_mrc > 35)  # Higher gross MRC increases churn probability
    )
    churn_prob = np.clip(churn_prob, 0, 1)  # Ensure probabilities are between 0 and 1
    has_churned = np.random.binomial(1, churn_prob)

    # Adjust mean to be around 0.03 by scaling down/up if necessary
    base_churn_prob = 0.03
    current_mean = has_churned.mean()
    logger.info(current_mean)
    scaling_factor = base_churn_prob / current_mean if current_mean != 0 else 1
    adjusted_churn_prob = np.clip(churn_prob * scaling_factor, 0, 1)
    has_churned = np.random.binomial(1, adjusted_churn_prob)

    label = pd.DataFrame(
        {
            "rating_account_id": core_data.rating_account_id,
            "has_churned": has_churned,
        }
    )

    return label


@asset(
    group_name=group_name,
    automation_condition=AutomationCondition.eager()
)
def bills(rating_account_id):
    rating_account_ids = rating_account_id["rating_account_id"].values

    date_range = pd.date_range(start="2024-04-01", end="2024-07-01", freq="MS")
    billed_period_month_ds = date_range.strftime("%Y-%m-%d")

    index = pd.MultiIndex.from_product(
        [rating_account_ids, billed_period_month_ds], names=["rating_account_id", "billed_period_month_d"]
    )
    num_rows = len(index)

    bills = pd.DataFrame(index=index).reset_index()

    # Generate 'has_used_roaming' (binary 0 or 1, 70% are 0)
    bills["has_used_roaming"] = np.random.choice([0, 1], size=len(bills), p=[0.7, 0.3])

    # Generate 'used_gb' (float between 0 and 70, can be 0)
    used_gb_distribution = np.concatenate(
        [
            np.random.uniform(0, 1, int(num_rows * 0.25)),
            np.random.uniform(1, 5, int(num_rows * 0.25)),
            np.random.uniform(5, 15, int(num_rows * 0.25)),
            np.random.uniform(15, 70, int(num_rows * 0.25)),
        ]
    )
    bills["used_gb"] = np.round(np.clip(used_gb_distribution, 0, 70), 1)

    # Generate 'has_used_gb' based on 'used_gb'
    bills["has_used_gb"] = (bills["used_gb"] > 1).astype(int)

    return bills


@asset(
    group_name=group_name,
    automation_condition=AutomationCondition.eager()
)
def aggregated_bills(bills):
    aggregated_bills = (
        bills.groupby("rating_account_id")
        .agg(has_used_roaming=("has_used_roaming", "max"), used_gb=("used_gb", "sum"), has_used_gb=("has_used_gb", "max"))
        .reset_index()
    )

    return aggregated_bills


@asset(
    group_name=group_name,
    automation_condition=AutomationCondition.eager()
)
def customer_interactions(unique_customer_ids):
    # Randomly select 50% of customer IDs without replacement
    customer_ids = unique_customer_ids["customer_id"].values
    num_unique_customers = len(customer_ids)
    selected_num = int(num_unique_customers * 0.5)
    selected_customer_ids = np.random.choice(customer_ids, size=selected_num, replace=False)

    # List of 'type_subtype' values
    type_subtypes = [
        "produkte&services-tarifdetails",
        "produkte&services-tarifwechsel",
        "rechnungsanfragen",
        "vvl",
    ]

    # For each customer, assign a random number of topics (1 to 3)
    num_topics = np.random.choice(a=[1, 2, 3], size=selected_num, p=[0.6, 0.3, 0.1])

    # Create a DataFrame with 'customer_id' and 'type_subtype'
    df_cases = pd.DataFrame(
        {
            "customer_id": np.repeat(selected_customer_ids, num_topics),
            "type_subtype": np.concatenate([np.random.choice(type_subtypes, size=n, replace=False) for n in num_topics]),
        }
    )

    # Generate 'n' values with specified distribution
    n_values = np.random.choice(
        a=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], size=len(df_cases), p=[0.5, 0.3, 0.1, 0.05, 0.02, 0.01, 0.005, 0.005, 0.005, 0.005]
    )
    df_cases["n"] = n_values

    # Generate 'days_since_last' between 0 and 180
    df_cases["days_since_last"] = np.random.randint(0, 181, size=len(df_cases))

    return df_cases


@asset(
    group_name=group_name,
    automation_condition=AutomationCondition.eager()
)
def pivoted_customer_interactions(customer_interactions):
    customer_interactions = customer_interactions.set_index("customer_id")

    df_cases_piv = (
        customer_interactions.pivot(columns="type_subtype", values=["n", "days_since_last"])
        .assign(n_cases=lambda df: df.n.sum(axis=1))
        .assign(days_since_last_case=lambda df: df.days_since_last.min(axis=1))
    )

    # Fill na using MultiIndex
    df_cases_piv.n = df_cases_piv.n.fillna(0)

    # Remove MultiIndex
    df_cases_piv.columns = ["_case_".join(col) if col[1] != "" else col[0] for col in df_cases_piv.columns]

    return df_cases_piv


@asset(
    group_name=group_name,
    automation_condition=AutomationCondition.eager()
)
def raw_features(core_data, aggregated_bills, pivoted_customer_interactions):
    raw_features = core_data.merge(aggregated_bills, on="rating_account_id", how="left").merge(
        pivoted_customer_interactions, on="customer_id", how="left"
    )

    # Selecting only the required columns
    raw_features = raw_features[
        [
            "rating_account_id",
            "customer_id",
            "age",
            "contract_lifetime_days",
            "remaining_binding_days",
            "has_special_offer",
            "is_magenta1_customer",
            "available_gb",
            "gross_mrc",
            "smartphone_brand",
            "has_used_roaming",
            "used_gb",
            "has_used_gb",
            "n_cases",
            "days_since_last_case",
            "n_case_produkte&services-tarifdetails",
            "n_case_produkte&services-tarifwechsel",
            "n_case_rechnungsanfragen",
            "n_case_vvl",
            "days_since_last_case_produkte&services-tarifdetails",
            "days_since_last_case_produkte&services-tarifwechsel",
            "days_since_last_case_rechnungsanfragen",
            "days_since_last_case_vvl",
        ]
    ]

    return raw_features


@asset(
    group_name=group_name,
    automation_condition=AutomationCondition.eager()
)
def features(raw_features):
    """
    - create perc_used_gb
    - reduce number of categories in smartphone_brand, and introduce Other category
    """

    """
    if available_gb=0, then perc_used_gb=used_gb to enhance the fact
    that the customer is using GB even if they are not included
    in the tariff.
    to set it to 0 is misleading
    to set it to NaN is a waste of info
    to set it to inf is not possible because xgboost will raise an error
    """
    features = raw_features.assign(
        available_gb=lambda df: df["available_gb"].fillna(0),
        perc_used_gb=lambda df: np.where(df["available_gb"] != 0, df["used_gb"] / df["available_gb"], df["used_gb"]),
        # Create categories for smartphone_brand
        smartphone_brand=lambda df: np.select(
            [
                df.smartphone_brand.isnull(),
                df.smartphone_brand.str.lower().isin(["samsung", "apple"]),
                df.smartphone_brand.str.lower().isin(["huawei", "xiaomi"]),
            ],
            [
                np.nan,
                df.smartphone_brand,
                "Huawei, Xiaomi",
            ],
            "Other",
        ),
    )
    logger.info("Remove customer id")
    features = features.drop("customer_id", axis=1)
    # Index will be insterted in the BigQuery table as first column (lower case).
    # But it is not recognised as index in future assets.
    logger.info("Number of records in the final dataset: %d", len(features))

    return features
