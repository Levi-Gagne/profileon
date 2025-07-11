# profile_option_docs.py

from config.cla_color_config import CLAColor as color

profile_option_docs = {
    "round": {
        "short": "Round min/max values for numeric columns.",
        "default": True,
        "long": f"""
<div style="font-family: Arial, sans-serif; color: {color.cla['navy'][5:]}; background: {color.cla['cloud'][5:]}; padding: 18px 22px; border-radius: 10px; border: 2px solid {color.cla['riptide'][5:]}; margin-bottom: 24px;">
  <h3 style="color: {color.cla['riptide'][5:]}; margin-top:0; margin-bottom: 5px;">round <span style="font-weight:400; color:{color.cla['smoke'][5:]}; font-size:16px;">(bool, default: True)</span></h3>
  <p><b>Rounds</b> the min and max values for numeric columns before generating rules. This helps when downstream consumers expect integer boundaries or when minor floating-point values aren't meaningful for the business.</p>
  <div style="margin:12px 0 6px 0;">
    <span style="color:{color.cla['celadon'][5:]}; font-weight:600;">Example:</span>
    If your data's min is <span style="color:{color.cla['saffron'][5:]};">0.2</span> and max is <span style="color:{color.cla['scarlett'][5:]};">9.8</span>, <br>
    with <b>round=True</b>: min becomes <span style="color:{color.cla['saffron'][5:]};">0</span>, max becomes <span style="color:{color.cla['scarlett'][5:]};">10</span>.
  </div>
</div>
"""
    },

    "max_in_count": {
        "short": "Threshold for is_in rule by number of distinct values.",
        "default": 10,
        "long": f"""
<div style="font-family: Arial, sans-serif; color: {color.cla['navy'][5:]}; background: {color.cla['cloud'][5:]}; padding: 18px 22px; border-radius: 10px; border: 2px solid {color.cla['celadon'][5:]}; margin-bottom: 24px;">
  <h3 style="color: {color.cla['celadon'][5:]}; margin-top:0; margin-bottom: 5px;">max_in_count <span style="font-weight:400; color:{color.cla['smoke'][5:]}; font-size:16px;">(int, default: 10, min: 1)</span></h3>
  <p>Creates an <b>is_in</b> rule if the number of distinct values in a column is less than this count.</p>
  <ul style="margin-left:0; padding-left:20px;">
    <li>Set low for highly categorical fields (e.g. enums, status).</li>
    <li>High values can slow down rule checks and are rarely useful.</li>
    <li>Min: 1</li>
  </ul>
  <div style="margin:12px 0 6px 0;">
    <span style="color:{color.cla['celadon'][5:]}; font-weight:600;">Example:</span>
    If a "status" column has only A, B, C and <b>max_in_count</b> is 10, an <b>is_in</b> rule will be created to enforce those values.
  </div>
</div>
"""
    },

    "distinct_ratio": {
        "short": "is_in rule if ratio of unique to total is below this.",
        "default": 0.05,
        "long": f"""
<div style="font-family: Arial, sans-serif; color: {color.cla['navy'][5:]}; background: {color.cla['cloud'][5:]}; padding: 18px 22px; border-radius: 10px; border: 2px solid {color.cla['celadon'][5:]}; margin-bottom: 24px;">
  <h3 style="color: {color.cla['celadon'][5:]}; margin-top:0; margin-bottom: 5px;">distinct_ratio <span style="font-weight:400; color:{color.cla['smoke'][5:]}; font-size:16px;">(float, 0–1, default: 0.05)</span></h3>
  <p>For columns with a limited set of allowed values. Generates an <b>is_in</b> rule if the fraction of unique values to total values is below this threshold.</p>
  <ul style="margin-left:0; padding-left:20px;">
    <li>Range: 0–1 (must be &gt;0 and ≤1)</li>
    <li>Use together with <b>max_in_count</b> for tight categorical checks.</li>
  </ul>
  <div style="margin:12px 0 6px 0;">
    <span style="color:{color.cla['celadon'][5:]}; font-weight:600;">Example:</span>
    1,000 rows, 10 unique values. Ratio = 0.01. If <b>distinct_ratio=0.05</b>, rule is created.
  </div>
</div>
"""
    },

    "max_null_ratio": {
        "short": "Max null ratio allowed before requiring is_not_null.",
        "default": 0.01,
        "long": f"""
<div style="font-family: Arial, sans-serif; color: {color.cla['navy'][5:]}; background: {color.cla['cloud'][5:]}; padding: 18px 22px; border-radius: 10px; border: 2px solid {color.cla['riptide'][5:]}; margin-bottom: 24px;">
  <h3 style="color: {color.cla['riptide'][5:]}; margin-top:0; margin-bottom: 5px;">max_null_ratio <span style="font-weight:400; color:{color.cla['smoke'][5:]}; font-size:16px;">(float, 0–1, default: 0.01)</span></h3>
  <p>Creates an <b>is_not_null</b> rule if the ratio of nulls in a column is below this threshold.</p>
  <ul style="margin-left:0; padding-left:20px;">
    <li>Range: 0–1</li>
    <li>Enforces not null when columns are mostly filled.</li>
    <li>If the column has more nulls than this, rule is NOT enforced.</li>
  </ul>
  <div style="margin:12px 0 6px 0;">
    <span style="color:{color.cla['celadon'][5:]}; font-weight:600;">Example:</span>
    5,000 rows, 2 nulls: ratio=0.0004. If <b>max_null_ratio=0.01</b>, <b>is_not_null</b> will be required.
  </div>
</div>
"""
    },

    "remove_outliers": {
        "short": "Apply statistical filtering to ignore outliers in profiling.",
        "default": True,
        "long": f"""
<div style="font-family: Arial, sans-serif; color: {color.cla['navy'][5:]}; background: {color.cla['cloud'][5:]}; padding: 18px 22px; border-radius: 10px; border: 2px solid {color.cla['celadon'][5:]}; margin-bottom: 24px;">
  <h3 style="color: {color.cla['celadon'][5:]}; margin-top:0; margin-bottom: 5px;">remove_outliers <span style="font-weight:400; color:{color.cla['smoke'][5:]}; font-size:16px;">(bool, default: True)</span></h3>
  <p>If <b>True</b>, statistical outliers are removed when generating min/max and other rules for numeric columns.</p>
  <ul style="margin-left:0; padding-left:20px;">
    <li>Helps rules reflect the true range for 99%+ of your data.</li>
    <li>False only if you want every possible value included, even clear errors.</li>
  </ul>
  <div style="margin:12px 0 6px 0;">
    <span style="color:{color.cla['celadon'][5:]}; font-weight:600;">Example:</span>
    For column "amount" with most values 1–100, but a few in the millions,<br>
    <b>remove_outliers=True</b> prevents skewed rule boundaries.
  </div>
</div>
"""
    },

    "outlier_columns": {
        "short": "Columns to apply outlier removal (empty = all numeric).",
        "default": [],
        "long": f"""
<div style="font-family: Arial, sans-serif; color: {color.cla['navy'][5:]}; background: {color.cla['cloud'][5:]}; padding: 18px 22px; border-radius: 10px; border: 2px solid {color.cla['navy'][5:]}; margin-bottom: 24px;">
  <h3 style="color: {color.cla['navy'][5:]}; margin-top:0; margin-bottom: 5px;">outlier_columns <span style="font-weight:400; color:{color.cla['smoke'][5:]}; font-size:16px;">(list of str, default: [])</span></h3>
  <p>Only apply outlier filtering to specific columns. Leave empty to apply to all numeric columns.</p>
  <ul style="margin-left:0; padding-left:20px;">
    <li>Use for high-variance columns; don’t use for categorical or ID columns.</li>
    <li>Can be set to one or more column names (case-sensitive).</li>
  </ul>
  <div style="margin:12px 0 6px 0;">
    <span style="color:{color.cla['celadon'][5:]}; font-weight:600;">Example:</span>
    <code style="color:{color.cla['celadon'][5:]};">['revenue', 'cost']</code>
  </div>
</div>
"""
    },

    "num_sigmas": {
        "short": "Std deviations for outlier filtering (higher = fewer outliers removed).",
        "default": 3,
        "long": f"""
<div style="font-family: Arial, sans-serif; color: {color.cla['navy'][5:]}; background: {color.cla['cloud'][5:]}; padding: 18px 22px; border-radius: 10px; border: 2px solid {color.cla['celadon'][5:]}; margin-bottom: 24px;">
  <h3 style="color: {color.cla['celadon'][5:]}; margin-top:0; margin-bottom: 5px;">num_sigmas <span style="font-weight:400; color:{color.cla['smoke'][5:]}; font-size:16px;">(int, default: 3, min: 1)</span></h3>
  <p>Number of standard deviations used to define outlier boundaries. Higher means more is kept.</p>
  <ul style="margin-left:0; padding-left:20px;">
    <li>Default of 3 is typical for most business use.</li>
    <li>Range: integer ≥1.</li>
    <li>Set lower for stricter outlier removal.</li>
  </ul>
  <div style="margin:12px 0 6px 0;">
    <span style="color:{color.cla['celadon'][5:]}; font-weight:600;">Example:</span>
    <b>num_sigmas=2</b> removes values >2 stddev from the mean; <b>num_sigmas=5</b> keeps almost everything.
  </div>
</div>
"""
    },

    "trim_strings": {
        "short": "Trim whitespace on both ends of string columns before profiling.",
        "default": True,
        "long": f"""
<div style="font-family: Arial, sans-serif; color: {color.cla['navy'][5:]}; background: {color.cla['cloud'][5:]}; padding: 18px 22px; border-radius: 10px; border: 2px solid {color.cla['riptide'][5:]}; margin-bottom: 24px;">
  <h3 style="color: {color.cla['riptide'][5:]}; margin-top:0; margin-bottom: 5px;">trim_strings <span style="font-weight:400; color:{color.cla['smoke'][5:]}; font-size:16px;">(bool, default: True)</span></h3>
  <p>Remove all whitespace from the start and end of each string before profiling. This is best practice for nearly all text columns except where whitespace is semantically important (rare).</p>
  <div style="margin:12px 0 6px 0;">
    <span style="color:{color.cla['celadon'][5:]}; font-weight:600;">Example:</span>
    <code style="color:{color.cla['celadon'][5:]};">'  apple '</code> is treated as <code style="color:{color.cla['celadon'][5:]};">'apple'</code>
  </div>
</div>
"""
    },

    "max_empty_ratio": {
        "short": "Max ratio of empty strings before is_not_null_or_empty is enforced.",
        "default": 0.01,
        "long": f"""
<div style="font-family: Arial, sans-serif; color: {color.cla['navy'][5:]}; background: {color.cla['cloud'][5:]}; padding: 18px 22px; border-radius: 10px; border: 2px solid {color.cla['saffron'][5:]}; margin-bottom: 24px;">
  <h3 style="color: {color.cla['saffron'][5:]}; margin-top:0; margin-bottom: 5px;">max_empty_ratio <span style="font-weight:400; color:{color.cla['smoke'][5:]}; font-size:16px;">(float, 0–1, default: 0.01)</span></h3>
  <p>If the ratio of empty strings in a column is below this threshold, generate an <b>is_not_null_or_empty</b> rule.</p>
  <ul style="margin-left:0; padding-left:20px;">
    <li>For text fields that should never be blank but may have rare blanks (legacy data, migration, etc).</li>
    <li>Range: 0–1</li>
  </ul>
  <div style="margin:12px 0 6px 0;">
    <span style="color:{color.cla['celadon'][5:]}; font-weight:600;">Example:</span>
    10,000 rows, 50 empty. Ratio=0.005.<br>
    If <b>max_empty_ratio=0.01</b>, <b>is_not_null_or_empty</b> will be enforced.
  </div>
</div>
"""
    },

    "sample_fraction": {
        "short": "Fraction of data sampled for profiling (speed/performance).",
        "default": 0.3,
        "long": f"""
<div style="font-family: Arial, sans-serif; color: {color.cla['navy'][5:]}; background: {color.cla['cloud'][5:]}; padding: 18px 22px; border-radius: 10px; border: 2px solid {color.cla['riptide'][5:]}; margin-bottom: 24px;">
  <h3 style="color: {color.cla['riptide'][5:]}; margin-top:0; margin-bottom: 5px;">sample_fraction <span style="font-weight:400; color:{color.cla['smoke'][5:]}; font-size:16px;">(float, 0–1, default: 0.3)</span></h3>
  <p>Percentage of data to use when profiling, as a float from 0–1.</p>
  <ul style="margin-left:0; padding-left:20px;">
    <li>Set lower for massive tables to speed up profiling.</li>
    <li>Set to 1.0 to always use all rows.</li>
  </ul>
  <div style="margin:12px 0 6px 0;">
    <span style="color:{color.cla['celadon'][5:]}; font-weight:600;">Example:</span>
    Table has 10M rows. <b>sample_fraction=0.05</b> → only 500,000 are sampled.
  </div>
</div>
"""
    },

    "sample_seed": {
        "short": "Random seed for reproducible sampling. None=random.",
        "default": None,
        "long": f"""
<div style="font-family: Arial, sans-serif; color: {color.cla['navy'][5:]}; background: {color.cla['cloud'][5:]}; padding: 18px 22px; border-radius: 10px; border: 2px solid {color.cla['charcoal'][5:]}; margin-bottom: 24px;">
  <h3 style="color: {color.cla['charcoal'][5:]}; margin-top:0; margin-bottom: 5px;">sample_seed <span style="font-weight:400; color:{color