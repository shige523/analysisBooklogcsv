import pandas as pd

"""
[ブクログでexpしたcsvデータを年月日別＆ジャンル別に集計]
[参考URL]
--pandas.DataFrame, Seriesを時系列データとして処理
https://note.nkmk.me/python-pandas-time-series-datetimeindex/
--pandasで時系列データの曜日や月、四半期、年ごとの合計や平均を算出
https://note.nkmk.me/python-pandas-time-series-multiindex/
--Pandasのgroupbyを使った要素をグループ化して処理をする方法
https://deepage.net/features/pandas-groupby.html
--pandas の SettingWithCopyWarning で苦労した話
https://qiita.com/HEM_SP/items/56cd62a1c000d342bd70
--pandasのgroupbyでグループ化した文字列を結合する
https://qiita.com/ground0state/items/2fb8d1938220df8a0194
"""


def agg_execute(filename):

    csv_header = [
        "サービスID",
        "アイテムID",
        "13桁ISBN",
        "カテゴリ",
        "評価",
        "読書状況",
        "レビュー",
        "タグ",
        "読書メモ(非公開)",
        "登録日時",
        "読了日",
        "タイトル",
        "作者名",
        "出版社名",
        "発行年",
        "ジャンル",
        "ページ数",
    ]
    book_log_df = pd.read_csv(
        filename,
        names=csv_header,
        encoding="shift_jis",
    )

    """
    データクリーニング
    欲しいデータは読み終わった＆読了日を設定しているデータ
    """
    filter_df = book_log_df[
        (book_log_df["読書状況"] == "読み終わった")
        & (book_log_df["読了日"] != None)
        & (book_log_df["読了日"] != "0000-00-00 00:00:00")
    ].copy()

    # 読了日を時系列データとして設定
    filter_df["読了日"] = pd.to_datetime(filter_df["読了日"])
    filter_df = filter_df.set_index("読了日")

    # 時系列のインデックスを設定
    df_m = filter_df.set_index(
        [filter_df.index.year, filter_df.index.month, filter_df.index]
    )
    df_m.index.names = ["year", "month", "読了日"]

    # 月ごとのジャンル内訳
    df_m_by_genre = (
        df_m[["ジャンル"]].groupby(["year", "month", "ジャンル"]).size().sort_index()
    )
    df_m_by_genre.name = "冊"
    df_m_by_genre.to_csv("result_count_by_genre_yyyymm.csv", encoding="utf-8")

    # 月ごとのページ数合計
    df_m_by_page = df_m["ページ数"].sum(level=("year", "month")).sort_index()
    df_m_by_page.to_csv("result_page_sum.csv", encoding="utf-8")

    # 月ごとのタイトル一覧
    df_m_by_title = df_m["タイトル"].sum(level=("year", "month"))
    df_m_by_title = df_m["タイトル"].groupby(level=("year", "month")).apply(list)
    df_m_by_title.to_csv("result_title_sum.csv", encoding="utf-8")


if __name__ == "__main__":
    # 読み込むcsvを指定
    agg_execute("booklog.csv")
