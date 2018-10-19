# sudo apt-get install python3-pip
# sudo python3 -m pip install pip --upgrade
# sudo pip install pandas
# sudo python3 -m pip install py2neo
# wget https://www.dropbox.com/s/z53qgw23lw042ij/BF201808.csv
import pandas as pd
from py2neo import *
import sys


class BolsaFamilia:
	GRAPH = Graph(password="ifg")

	def cleanDatabase(self):
		self.GRAPH.run("MATCH (n) DETACH DELETE n;")

	def loadDatabase(self):
		for dataframe in pd.read_csv('BF201808.csv', sep=";", chunksize=10 ** 6):
			for index, linha in dataframe.iterrows():
				sys.stdout.write('.')
				tx = self.GRAPH.begin()
				p = Node("Pagamento", valor=linha['VALOR PARCELA'], mesReferencia=linha['MÊS REFERÊNCIA'],
					 mesCompetencia=linha['MÊS COMPETÊNCIA'])
				h = Node("Beneficiário", nome=linha['NOME FAVORECIDO'], nis=linha['NIS FAVORECIDO'], uf=linha['UF'],
					 codMunicipio=linha['CÓDIGO MUNICÍPIO SIAFI'], nomeMunicipio=linha['NOME MUNICÍPIO'])
				tx.create(p)
				tx.merge(h, "Beneficiário", "nis")
				p_h = Relationship(h, "RECEBE", p)
				tx.create(p_h)
				tx.commit()

bf = BolsaFamilia()
bf.cleanDatabase()
bf.loadDatabase()
