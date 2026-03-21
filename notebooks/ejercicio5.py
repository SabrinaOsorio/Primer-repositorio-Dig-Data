from mrjob.job import MRJob

class PromedioActas(MRJob):

    def mapper(self, _, linea):
        partes = linea.strip().split(',')
        if len(partes) == 3:
            candidato = partes[0]
            votos = int(partes[2])

            # INICIA TU CODIGO AQUI

            # Emitimos (votos, 1) → (suma parcial, conteo)
            yield candidato, (votos, 1)

            # TERMINA TU CODIGO AQUI

    def reducer(self, candidato, valores_tuplas):
        suma_total = 0
        cantidad_mesas = 0

        # INICIA TU CODIGO AQUI

        # Recorremos todas las tuplas
        for votos, un_acta in valores_tuplas:
            suma_total += votos        # acumulamos votos
            cantidad_mesas += un_acta # acumulamos número de mesas

        promedio = suma_total / cantidad_mesas
        yield candidato, round(promedio, 2)

        # TERMINA TU CODIGO AQUI

if __name__ == '__main__':
    PromedioActas.run()
