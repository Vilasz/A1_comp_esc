class DataFrame:
    def __init__(self, columns : list[str], rows : list[list]:
        self.columns = columns # lista de str com o nome das colunas
        self.rows = rows  # lista de listas ou lista de dicionários

    def to_dicts(self):
        """
        Retorna uma versão do DataFrame como um dicionário.

        Saída:
            dict: dicionário onde as chaves são os nomes das colunas
            e os valores são as linhas do DataFrame.
        """
        return [dict(zip(self.columns, row)) for row in self.rows]

    def add_row(self, row : list):
        """
        Adiciona a linha ao DataFrame.

        Entrada:
            row (list): uma lista de formato equivalente às linhas
            do DataFrame.
        """
        if len(row) != len(self.columns):
            raise ValueError("Número de colunas incorreto.")
        self.rows.append(row)

    def vstack(self, df : DataFrame):
        """
        Dado outro DataFrame com o mesmo número de colunas, empilha
        ele abaixo do DataFrame em que o método está sendo aplicado.

        Entrada:
            df(DataFrame): DataFrame a ser empilhado abaixo.
        """
        if len(df.columns) != len(self.columns):
            raise ValueError("Número de colunas incorreto.")
        self.rows.extend(df.rows)
    
    def shape(self):
        """
        Retorna uma tupla com o número de linhas e colunas do DataFrame

        Saída:
            Tuple(int,int): (Número de linhas, Número de colunas)
        """
        return (len(self.rows), len(self.columns))

    def group_by(self, column_name : str, agg_fn : function, agg_columns=None : list[str]):
        """
        Retorna uma versão agrupada do DataFrame pela coluna column_name,
        aplicando a função agg_fn nas colunas indicadas em agg_columns e
        descartando as que não foram indicadas. Se agg_columns = None 
        agg_fn é aplicada em todas as colunas.

        Entrada:
            column_name(str): Nome da coluna que servirá de referencia
            para o agrupamento.
            agg_fn: Função que sumarizará as colunas não agrupadas
            indicadas em agg_columns.
            agg_columns(list[str]): Lista das colunas que são sumarizadas.

        Saída:
            DataFrame: DataFrame após o agrupamento feito.
        """
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

    def sort_by(self, column_name : str, ascending=True : bool):
        """
        Ordena as linhas do DataFrame com relação a coluna column_name.
        Se ascending = True a ordenação é crescente se = False é 
        decrescente.

        Entrada:
            column_name(str): Nome da coluna de referência para ordenação
            das linha do DataFrame.
            ascending(bool): True para ordenação crescente e False para
            descrescente.

        Saída:
            DataFrame: DataFrame com as linhas ordenadas.
        """
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



