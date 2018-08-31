# sudo apt-get install python3-pip
# sudo pip install pandas
# sudo python3 -m pip install pip --upgrade
# sudo python3 -m pip install py2neo
from py2neo import *
import pandas as pd

class TopicosI:

	GRAPH = Graph(password="ifg")

	def cleanDatabase(self):
		self.GRAPH.run("MATCH (n) DETACH DELETE n;")

	def loadDatabase(self):
		df = pd.read_csv('data.txt', sep=',')
		print(df)
		for index, linha in df.iterrows():
			tx = self.GRAPH.begin()
			p = Node("Pessoa", nome=linha['pessoa'])
			h = Node("Hobbie", nome=linha['hobbie'])
			t = Node("Profissao", nome=linha['profissao'])
			tx.create(p)
			tx.merge(h, "Hobbie", "nome")
			tx.merge(t, "Profissao", "nome")
			p_h = Relationship(p, "GOSTA_DE", h)
			p_t = Relationship(p, "QUER_SER", t)
			tx.create(p_h)
			tx.create(p_t)
			tx.commit()


	def getPessoaByProfissao(self, profissao):
		result = self.GRAPH.run("MATCH (p:Pessoa)-[:QUER_SER]-(t:Profissao) WHERE t.nome='"+profissao+"' RETURN p.nome AS nome").data()
		return result

topicos  = TopicosI()
topicos.cleanDatabase()
topicos.loadDatabase()
profissao = "Programador"
result = topicos.getPessoaByProfissao(profissao)
for pessoa in result:
	print("O nome da pessoa que quer ser um "+profissao+" é "+pessoa['nome'])