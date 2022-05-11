import difflib
import json
import pathlib
from typing import *

import pandas as pd


def normalize(s: str) -> Any:
    s = s.strip().lower()
    return s


def diff_func(name1: str, name2: str) -> float:
    diff = difflib.SequenceMatcher(None, name1, name2)

    ratio = diff.ratio()

    return ratio


def find_closest_match(
    name1: str, input_merge_column: str, school_authorities: list[dict]
) -> pd.Series:
    matches: list[dict] = []

    for n, d in enumerate(school_authorities):
        key = d["key"]
        df = d["df"]
        merge_column = d["merge_column"]

        column: pd.Series = df[key]

        ratios = column.apply(lambda name2: diff_func(name1, normalize(name2)))
        max_ratio_ix = ratios.argmax()

        matched_key = column[max_ratio_ix]
        ratio = ratios[max_ratio_ix]

        match = {
            input_merge_column: df.loc[max_ratio_ix, merge_column],
            "ratio": ratio,
            "index": n,
            "matched_key": matched_key,
        }

        if ratio >= 0.99:
            return match
        else:
            matches.append(match)

    return max(matches, key=lambda y: y["ratio"])


out_dirpath = pathlib.Path("./out/")

school_authorities = json.load(open("src/authorities.json", "r"))
school_authorities = [
    {**i, "df": pd.read_csv(i["filepath"])} for i in school_authorities
]

input_filepath = (
    "src/K12 and Community College IP Addressing_2022_04_18 - 2022-04-18.csv"
)
input_key = "MCNC Entity Name"
input_merge_column = "inst_id"

out_filepath = out_dirpath.joinpath("matched.xlsx")

writer = pd.ExcelWriter(out_filepath, engine="xlsxwriter")

df = pd.read_csv(input_filepath)

mappings: dict[str, Any] = {}


def func(x: pd.Series) -> pd.Series:
    name1 = normalize(x[input_key])

    if name1 not in mappings:
        max_match = find_closest_match(
            name1=name1,
            input_merge_column=input_merge_column,
            school_authorities=school_authorities,
        )
        mappings[name1] = pd.Series(max_match)

    y = mappings[name1]

    return pd.concat((x, y))


matched = df.apply(func, axis=1)
matched.to_excel(writer, "All", index=False)


for n, group_df in matched.groupby("index"):
    d = school_authorities[n]

    key = d["key"]
    df = d["df"]

    out_df = pd.merge(
        left=df,
        right=group_df,
        how="inner",
        left_on=key,
        right_on="matched_key",
    )

    filepath = pathlib.Path(d["filepath"])
    filename = filepath.stem[:30]

    out_df.to_excel(writer, sheet_name=filename, index=False)

writer.save()
