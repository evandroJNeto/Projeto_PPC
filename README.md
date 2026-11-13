# Projeto PPC

Alunos: Evandro José dos Santos Neto, Kelvin Fagundes Gomes de Souza e Mizael Paris Leite

Este repositório contém a Etapa 1, Etapa 2 e 3 do trabalho.

## Produtor-Consumidor (prod_cons.c)

Produtores e consumidores (3 de cada) trocam relógios vetoriais por uma fila de tamanho 5, sincronizada com mutex e variáveis de condição. Mude CENARIO no código para escolher:

1: fila cheia (produtores bloqueiam)
2: fila vazia (consumidores bloqueiam)

gcc -g -Wall -o prod_cons prod_cons.c -lpthread
./prod_cons

O programa roda em loop infinito; encerre com Ctrl+C.
Etapa 3) Código dos Relógios vetoriais foi adaptado para o produtor-consumidor a fim de integrar os dois
