import logging
import random
import sys

import numpy as np
import pandas as pd
from dagster import AssetOut, asset, get_dagster_logger, multi_asset

log_fmt = "[%(asctime)s] %(message)s"
log_datefmt = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(stream=sys.stdout, format=log_fmt, datefmt=log_datefmt, level=logging.INFO)
logger = get_dagster_logger(__name__)

group_name = "get_data"


@multi_asset(
    group_name=group_name,
    outs={
        "rating_account_id": AssetOut(),
        "unique_customer_ids": AssetOut(),
        "core_data": AssetOut(),
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

    # Generate 'has_done_upselling' with correlations and adjusted mean upsell rate
    upsell_prob = (
        0.2 * (age < 45)  # Younger customers might be more likely to accept upsell offers
        + 0.15 * ((contract_lifetime_days >= 300) & (contract_lifetime_days <= 365*4))  # Newer customers might be more open to upsell offers
        + 0.1 * (remaining_binding_days < 60)  # Customers with few binding days might look for new offers
        + 0.1 * (is_magenta1_customer == 1)  # Magenta1 customers might be more engaged and open to upsell
        + 0.2 * (np.array(available_gb, dtype=float) <= 20)  # Customers with less available GB might want more
        + 0.1 * (gross_mrc < 35)  # Customers with lower gross MRC might be interested in upgrading
    )

    upsell_prob = np.clip(upsell_prob, 0, 1)  # Ensure probabilities are between 0 and 1
    has_done_upselling = np.random.binomial(1, upsell_prob)

    # Adjust mean to be around 0.07 by scaling down/up if necessary
    base_upsell_prob = 0.07
    current_mean = has_done_upselling.mean()
    logger.info(current_mean)
    scaling_factor = base_upsell_prob / current_mean if current_mean != 0 else 1
    adjusted_upsell_prob = np.clip(upsell_prob * scaling_factor, 0, 1)
    has_done_upselling = np.random.binomial(1, adjusted_upsell_prob)

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
            "has_done_upselling": has_done_upselling,
        }
    )

    return rating_account_id, unique_customer_ids, core_data


@asset(
    group_name=group_name,
    
)
def usage_info(rating_account_id):
    # Create a DataFrame with all combinations of 'rating_account_id's and dates
    date_range = pd.date_range(start="2024-04-01", end="2024-07-01", freq="MS")
    billed_period_month_ds = date_range.strftime("%Y-%m-%d")

    # Create a MultiIndex from 'rating_account_id's and 'billed_period_month_d's
    index = pd.MultiIndex.from_product(
        [rating_account_id, billed_period_month_ds], names=["rating_account_id", "billed_period_month_d"]
    )
    num_rows = len(index)

    usage_info = pd.DataFrame(index=index).reset_index()

    # Generate 'has_used_roaming' (binary 0 or 1, 70% are 0)
    usage_info["has_used_roaming"] = np.random.choice([0, 1], size=len(usage_info), p=[0.7, 0.3])

    # Generate 'used_gb' (float between 0 and 70, can be 0)
    used_gb_distribution = np.concatenate(
        [
            np.random.uniform(0, 1, int(num_rows * 0.25)),
            np.random.uniform(1, 5, int(num_rows * 0.25)),
            np.random.uniform(5, 15, int(num_rows * 0.25)),
            np.random.uniform(15, 70, int(num_rows * 0.25)),
        ]
    )
    usage_info["used_gb"] = np.round(np.clip(used_gb_distribution, 0, 70), 1)

    return usage_info


@asset(
    group_name=group_name,
)
def customer_interactions(unique_customer_ids):
    # Randomly select 50% of customer IDs without replacement
    num_unique_customers = len(unique_customer_ids)
    selected_num = int(num_unique_customers * 0.5)
    selected_customer_ids = np.random.choice(unique_customer_ids, size=selected_num, replace=False)

    # List of 'type_subtype' values
    type_subtypes = [
        "produkte&services-tarifdetails",
        "produkte&services-tarifwechsel",
        "rechnungsanfragen",
        "prolongation",
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
