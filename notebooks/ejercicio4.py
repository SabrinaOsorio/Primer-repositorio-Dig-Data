from mrjob.job import MRJob

class MaximoPorCandidato(MRJob):

    def mapper(self, _, linea):
        partes = linea.strip().split(',')
        if len(partes) == 3:
            candidato = partes[0]
            votos = int(partes[2])  # Cuidado: convertir texto a entero

            # INICIA TU CODIGO AQUI

            # Emitimos (candidato, votos de esa mesa)
            yield candidato, votos

            # TERMINA TU CODIGO AQUI

    def reducer(self, candidato, votos_de_todas_las_mesas):
        # INICIA TU CODIGO AQUI

        # Obtenemos el valor máximo
        record = max(votos_de_todas_las_mesas)

        # Emitimos (candidato, máximo de votos en una mesa)
        yield candidato, record

        # TERMINA TU CODIGO AQUI

if __name__ == '__main__':
    MaximoPorCandidato.run()
