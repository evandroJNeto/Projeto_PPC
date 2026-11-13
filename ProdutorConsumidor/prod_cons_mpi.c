/*
 * Compilação: mpicc -g -Wall -o prod_cons_mpi prod_cons_mpi.c -lpthread
 * Execução:   mpiexec -n 3 --oversubscribe ./prod_cons_mpi
 */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>

#define BUFFER_SIZE 5
#define NUM_PRODUTORES 3
#define NUM_CONSUMIDORES 3

#define CENARIO 1

/* Estrutura do relógio vetorial */
typedef struct Clock {
    int p[3];
    int prod_id;    // Identificador para sabermos qual thread gerou
    char acao[50];  // String para identificar o tipo de evento MPI
} Clock;

Clock queue[BUFFER_SIZE];
int count = 0;

pthread_mutex_t mutex;
pthread_cond_t condFull;
pthread_cond_t condEmpty;

// Variáveis globais do MPI para as threads saberem quem são
int my_rank;
int comm_sz;

/* Extrai da fila protegida */
Clock getClock(int id) {
    pthread_mutex_lock(&mutex);

    while (count == 0) {
        printf("[P%d - Consumidor %d] Fila VAZIA! Condição bloqueando...\n", my_rank, id);
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

/* Insere na fila protegida */
void submitClock(Clock clock, int id) {
    pthread_mutex_lock(&mutex);

    while (count == BUFFER_SIZE) {
        printf("[P%d - Produtor %d] Fila CHEIA! Condição bloqueando...\n", my_rank, id);
        pthread_cond_wait(&condFull, &mutex);
    }

    queue[count] = clock;
    count++;

    pthread_mutex_unlock(&mutex);
    pthread_cond_signal(&condEmpty);
}

/* --- INTEGRAÇÃO DAS FUNÇÕES DO RELÓGIO VETORIAL COM MPI --- */

void Event(Clock *clock, int id_thread) {
    clock->p[my_rank]++;
    clock->prod_id = id_thread;
    sprintf(clock->acao, "Evento Interno");
    
    // O produtor submete o estado na fila
    submitClock(*clock, id_thread);
}

void Send(int dest, Clock *clock, int id_thread) {
    clock->p[my_rank]++;
    clock->prod_id = id_thread;
    sprintf(clock->acao, "Send para P%d", dest);
    
    // Envio MPI
    MPI_Send(clock->p, 3, MPI_INT, dest, 0, MPI_COMM_WORLD);
    
    // O produtor submete o estado na fila
    submitClock(*clock, id_thread);
}

void Receive(int src, Clock *clock, int id_thread) {
    int recebe[3];
    
    // Recebimento MPI
    MPI_Recv(recebe, 3, MPI_INT, src, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    
    clock->p[my_rank]++;
    for (int i = 0; i < 3; i++) {
        if (clock->p[i] < recebe[i]) {
            clock->p[i] = recebe[i];
        }
    }
    clock->prod_id = id_thread;
    sprintf(clock->acao, "Receive de P%d", src);
    
    // O produtor submete o estado na fila
    submitClock(*clock, id_thread);
}

/* Thread Produtora */
void* producerThread(void* arg) {
    int id = *(int*)arg;
    Clock clock = {{0, 0, 0}, id, ""};
    
    // Cálculo seguro de nós adjacentes para envio e recebimento em anel
    int next_proc = (my_rank + 1) % comm_sz;
    int prev_proc = (my_rank - 1 + comm_sz) % comm_sz;

    while (1) {
        // 1. Gera Evento Interno
        Event(&clock, id);
        if (CENARIO == 1) sleep(1); else sleep(2);

        // 2. Envia relógio para o vizinho da frente
        Send(next_proc, &clock, id);
        if (CENARIO == 1) sleep(1); else sleep(2);

        // 3. Recebe relógio do vizinho de trás
        Receive(prev_proc, &clock, id);
        if (CENARIO == 1) sleep(1); else sleep(2);
    }
    return NULL;
}

/* Thread Consumidora */
void* consumerThread(void* arg) {
    int id = *(int*)arg;

    while (1) {
        Clock clock = getClock(id);

        printf("   [P%d] [Consumidor %d] Leu da fila: [%-15s] -> Clock = (%d, %d, %d) [Produtor %d]\n",
                my_rank, id, clock.acao, clock.p[0], clock.p[1], clock.p[2], clock.prod_id);

        if (CENARIO == 1) sleep(2); else sleep(1);
    }
    return NULL;
}

int main(int argc, char* argv[]) {
    int provided;
    
    // MPI_Init_thread é crucial aqui, pois múltiplas threads farão chamadas MPI simultaneamente
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        printf("Aviso: A implementação MPI não suporta MPI_THREAD_MULTIPLE de forma nativa.\n");
    }
    
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);

    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&condEmpty, NULL);
    pthread_cond_init(&condFull, NULL);

    pthread_t produtores[NUM_PRODUTORES];
    pthread_t consumidores[NUM_CONSUMIDORES];
    int ids[3] = {0, 1, 2};

    if (my_rank == 0) {
        printf("\n--- INICIANDO EXECUÇÃO HÍBRIDA (MPI + Pthreads) : CENÁRIO %d ---\n\n", CENARIO);
    }
    
    // Pequeno sleep para garantir que o cabeçalho imprime antes das operações
    sleep(1);

    for (int i = 0; i < NUM_PRODUTORES; i++) {
        pthread_create(&produtores[i], NULL, producerThread, &ids[i]);
    }
    for (int i = 0; i < NUM_CONSUMIDORES; i++) {
        pthread_create(&consumidores[i], NULL, consumerThread, &ids[i]);
    }

    for (int i = 0; i < NUM_PRODUTORES; i++) {
        pthread_join(produtores[i], NULL);
    }
    for (int i = 0; i < NUM_CONSUMIDORES; i++) {
        pthread_join(consumidores[i], NULL);
    }

    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&condEmpty);
    pthread_cond_destroy(&condFull);
    
    MPI_Finalize();
    return 0;
}