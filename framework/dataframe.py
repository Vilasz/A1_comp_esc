class DataFrame:
    def __init__(self, columns, rows):
        self.columns = columns # lista de str com o nome das colunas
        self.rows = rows  # lista de listas ou lista de dicionários

    def to_dicts(self):
        return [dict(zip(self.columns, row)) for row in self.rows]

    def add_row(self, row):
        if len(row) != len(self.columns):
            raise ValueError("Número de colunas incorreto.")
        self.rows.append(row)
    
    def shape(self):
        return (len(self.rows), len(self.columns))

    def group_by(self, column_name, agg_fn, agg_columns=None):
        col_idx = self.columns.index(column_name)
        grouped = {}

        for row in self.rows:
            key = row[col_idx]
            grouped.setdefault(key, []).append(row)

        # Determina colunas a serem agregadas
        if agg_columns is None:
            agg_columns = [col for col in self.columns if col != column_name]
        agg_col_indices = [self.columns.index(col) for col in agg_columns]

        new_rows = []
        for key, group in grouped.items():
            agg_values = []
            for idx in agg_col_indices:
                values = [row[idx] for row in group]
                agg_values.append(agg_fn(values))
            new_rows.append([key] + agg_values)

        new_columns = [column_name] + [f"{agg_fn.__name__}({col})" for col in agg_columns]
        return DataFrame(new_columns, new_rows)

    def sort_by(self, column_name, ascending=True):
        col_idx = self.columns.index(column_name)
        sorted_rows = sorted(self.rows, key=lambda row: float(row[col_idx]), reverse=not ascending)
        return DataFrame(self.columns, sorted_rows)

    def __str__(self):
        return "\n".join(str(dict(zip(self.columns, row))) for row in self.rows)


if __name__ == "__main__":
    df = DataFrame(["1", "2", "3"], [[True,3213,"H"],[False,32,"H"], [True, 3244, "M"]])
    print(df.shape())
    print(df)
    print("===============")
    df.add_row([True,2123,"M"])
    print(df)
    print("===============")
    print(df.to_dicts())
    print(df.group_by("3",sum,"2"))



