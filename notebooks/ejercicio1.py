from mrjob.job import MRJob

class EscrutinioNacional(MRJob):

    def mapper(self, _, linea):
        voto = linea.strip()
        # INICIA TU CODIGO AQUI

        # Emitimos (candidato, 1)
        yield voto, 1

        # TERMINA TU CODIGO AQUI

    def reducer(self, candidato, conteos):
        # INICIA TU CODIGO AQUI

        # Sumamos todos los 1s
        total = sum(conteos)

        # Emitimos (candidato, total de votos)
        yield candidato, total

        # TERMINA TU CODIGO AQUI

if __name__ == '__main__':
    EscrutinioNacional.run()
