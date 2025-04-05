class DataFrame:
    def __init__(self, columns : list[str], data = []):
        self.columns = columns # lista de str com o nome das colunas
        self.data = data # lista de listas, onde cada lista é uma linha

    def add_row(self, row):
        if len(row) != len(self.columns):
            raise ValueError("Número de colunas incorreto.")
        self.data.append(row)

    def to_dicts(self):
        return [dict(zip(self.columns, row)) for row in self.data]

    def __len__(self):
        return len(self.data)

    def __str__(self):
        return "\n".join(str(dict(zip(self.columns, row))) for row in self.data)



if __name__ == "__main__":
    df = DataFrame(["1", "2", "3"], [[True,3213,"homem"],[False,32,"salve"], [True, 3244, "Mulher"]])
    print(len(df))
    print(df)
    print("===============")
    df.add_row([True,2123,"Muié"])
    print(df)
    print("===============")
    print(df.to_dicts())


