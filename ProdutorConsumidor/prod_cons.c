/*
 * Compilação: gcc -g -Wall -o prod_cons prod_cons.c -lpthread
 * Execução:   ./prod_cons
 *
 * Para trocar o cenário, altere o valor de CENARIO abaixo e recompile.
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

#define BUFFER_SIZE 5       
#define NUM_PRODUTORES 3    
#define NUM_CONSUMIDORES 3 

// DEFINA O CENÁRIO AQUI:
// 1 = Cenário Fila Cheia 
// 2 = Cenário Fila Vazia 
#define CENARIO 1

/* Estrutura do relógio vetorial*/
typedef struct Clock {
    int p[3];
    int prod_id;    // Identificador do produto p saber qual thread gerou
} Clock;

Clock queue[BUFFER_SIZE];
int count = 0;

pthread_mutex_t mutex;
pthread_cond_t condFull;  // Usado para bloquear produção quando cheia
pthread_cond_t condEmpty; // Usado para bloquear consumo quando vazia

/*
 * Função equivalente ao getTask() do pth_pool.c.
 * Remove um relógio da fila.
 */
Clock getClock(int id) {
    pthread_mutex_lock(&mutex);

    while (count == 0) {
        // Comprovação visual para o Cenário 2 (Fila Vazia)
        printf("[Consumidor %d] Fila VAZIA! Variável de condição bloqueando consumo...\n", id);
        pthread_cond_wait(&condEmpty, &mutex);
    }

    Clock clock = queue[0];
    for (int i = 0; i < count - 1; i++) {
        queue[i] = queue[i+1];
    }
    count--;

    pthread_mutex_unlock(&mutex);
    pthread_cond_signal(&condFull);

    return clock;
}

/*
 * Função equivalente ao submitTask() do pth_pool.c.
 * Adiciona um relógio na fila.
 */
void submitClock(Clock clock, int id) {
    pthread_mutex_lock(&mutex);

    while (count == BUFFER_SIZE) {
        // Comprovação visual para o Cenário 1 (Fila Cheia)
        printf("[Produtor %d] Fila CHEIA! Variável de condição bloqueando produção...\n", id);
        pthread_cond_wait(&condFull, &mutex);
    }

    queue[count] = clock;
    count++;

    pthread_mutex_unlock(&mutex);
    pthread_cond_signal(&condEmpty);
}

/* Thread Produtora */
void* producerThread(void* arg) {
    int id = *(int*)arg;
    Clock clock = {{0, 0, 0}, id}; // Inicializa vetor zerado

    while (1) {
        // Incrementa o relógio na posição da própria thread simulando um evento
        clock.p[id]++;

        submitClock(clock, id);
        printf("[Produtor %d] Produziu e enfileirou -> Clock = (%d, %d, %d)\n",
                id, clock.p[0], clock.p[1], clock.p[2]);

        // Controle de taxa de produção baseado nos cenários solicitados
        if (CENARIO == 1) {
            sleep(1); // Produz rápido (1s)
        } else {
            sleep(2); // Produz lento (2s)
        }
    }
    return NULL;
}

/* Thread Consumidora */
void* consumerThread(void* arg) {
    int id = *(int*)arg;

    while (1) {
        Clock clock = getClock(id);

        // Imprime a saída padrão dos relógios consumidos
        printf("   [Consumidor %d] Consumiu da fila -> Clock = (%d, %d, %d) [Gerado pelo Produtor %d]\n",
                id, clock.p[0], clock.p[1], clock.p[2], clock.prod_id);

        // Controle de taxa de consumo baseado nos cenários solicitados
        if (CENARIO == 1) {
            sleep(2); // Consome lento (2s)
        } else {
            sleep(1); // Consome rápido (1s)
        }
    }
    return NULL;
}

int main(int argc, char* argv[]) {
    // Inicialização do Mutex e Variáveis de Condição
    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&condEmpty, NULL);
    pthread_cond_init(&condFull, NULL);

    pthread_t produtores[NUM_PRODUTORES];
    pthread_t consumidores[NUM_CONSUMIDORES];
    int ids[3] = {0, 1, 2}; // IDs das threads 0, 1 e 2

    printf("\n--- INICIANDO EXECUÇÃO: CENÁRIO %d ---\n", CENARIO);
    if (CENARIO == 1) {
        printf("A fila FICARÁ CHEIA. Produtores gerarão relógios mais rápido que o consumo.\n\n");
    } else {
        printf("A fila FICARÁ VAZIA. Consumidores retirarão relógios mais rápido que a produção.\n\n");
    }

    // Criando grupo de 3 produtores e 3 consumidores
    for (int i = 0; i < NUM_PRODUTORES; i++) {
        pthread_create(&produtores[i], NULL, producerThread, &ids[i]);
    }
    for (int i = 0; i < NUM_CONSUMIDORES; i++) {
        pthread_create(&consumidores[i], NULL, consumerThread, &ids[i]);
    }

    // O programa roda num loop infinito por conta do while(1) das threads.
    for (int i = 0; i < NUM_PRODUTORES; i++) {
        pthread_join(produtores[i], NULL);
    }
    for (int i = 0; i < NUM_CONSUMIDORES; i++) {
        pthread_join(consumidores[i], NULL);
    }

    // Destruição do mutex e vars de condição ao finalizar
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&condEmpty);
    pthread_cond_destroy(&condFull);

    return 0;
}
