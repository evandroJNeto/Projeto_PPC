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
    int prod_id;    
    char acao[50];  
} Clock;

/* --- FILA COMPARTILHADA (PRODUTOR/CONSUMIDOR) --- */
Clock queue[BUFFER_SIZE];
int count = 0;
pthread_mutex_t mutex_fila;
pthread_cond_t condFull;
pthread_cond_t condEmpty;

/* --- RELÓGIO VETORIAL DO PROCESSO MPI --- */
// Pertence ao processo como um todo, protegido por um mutex próprio.
Clock relogio_processo = {{0, 0, 0}, -1, "Init"};
pthread_mutex_t mutex_relogio;

int my_rank;
int comm_sz;

/* --- FUNÇÕES DA FILA --- */
Clock getClock(int id) {
    pthread_mutex_lock(&mutex_fila);

    while (count == 0) {
        pthread_cond_wait(&condEmpty, &mutex_fila);
    }

    Clock clock = queue[0];
    for (int i = 0; i < count - 1; i++) {
        queue[i] = queue[i+1];
    }
    count--;

    pthread_mutex_unlock(&mutex_fila);
    pthread_cond_signal(&condFull);

    return clock;
}

void submitClock(Clock clock, int id) {
    pthread_mutex_lock(&mutex_fila);

    while (count == BUFFER_SIZE) {
        pthread_cond_wait(&condFull, &mutex_fila);
    }

    queue[count] = clock;
    count++;

    pthread_mutex_unlock(&mutex_fila);
    pthread_cond_signal(&condEmpty);
}

/* --- FUNÇÕES DO RELÓGIO VETORIAL (AGORA ACESSAM A VARIÁVEL GLOBAL) --- */

void Event(int id_thread) {
    pthread_mutex_lock(&mutex_relogio);
    
    relogio_processo.p[my_rank]++; // Incrementa o índice deste processo
    relogio_processo.prod_id = id_thread;
    sprintf(relogio_processo.acao, "Evento Interno");
    
    Clock copia_para_fila = relogio_processo; // Salva o estado atual
    pthread_mutex_unlock(&mutex_relogio);
    
    submitClock(copia_para_fila, id_thread);
}

void Send(int dest, int id_thread) {
    pthread_mutex_lock(&mutex_relogio);
    
    relogio_processo.p[my_rank]++;
    relogio_processo.prod_id = id_thread;
    sprintf(relogio_processo.acao, "Send para P%d", dest);
    
    int buffer_envio[3] = {relogio_processo.p[0], relogio_processo.p[1], relogio_processo.p[2]};
    Clock copia_para_fila = relogio_processo;
    pthread_mutex_unlock(&mutex_relogio);
    
    // O MPI_Send fica fora do mutex para não travar outras threads desnecessariamente
    MPI_Send(buffer_envio, 3, MPI_INT, dest, 0, MPI_COMM_WORLD);
    submitClock(copia_para_fila, id_thread);
}

void Receive(int src, int id_thread) {
    int recebe[3];
    
    // Aguarda receber a mensagem livremente
    MPI_Recv(recebe, 3, MPI_INT, src, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    
    // Ao receber, trava o relógio para fazer a sincronização
    pthread_mutex_lock(&mutex_relogio);
    relogio_processo.p[my_rank]++;
    for (int i = 0; i < 3; i++) {
        if (relogio_processo.p[i] < recebe[i]) {
            relogio_processo.p[i] = recebe[i];
        }
    }
    relogio_processo.prod_id = id_thread;
    sprintf(relogio_processo.acao, "Receive de P%d", src);
    
    Clock copia_para_fila = relogio_processo;
    pthread_mutex_unlock(&mutex_relogio);
    
    submitClock(copia_para_fila, id_thread);
}

/* --- THREADS PRODUTORAS E CONSUMIDORAS --- */

void* producerThread(void* arg) {
    int id = *(int*)arg;
    int next_proc = (my_rank + 1) % comm_sz;
    int prev_proc = (my_rank - 1 + comm_sz) % comm_sz;

    while (1) {
        Event(id);
        if (CENARIO == 1) sleep(1); else sleep(2);

        Send(next_proc, id);
        if (CENARIO == 1) sleep(1); else sleep(2);

        Receive(prev_proc, id);
        if (CENARIO == 1) sleep(1); else sleep(2);
    }
    return NULL;
}

void* consumerThread(void* arg) {
    int id = *(int*)arg;

    while (1) {
        Clock clock = getClock(id);

        // Formatação visual para separar os prints por colunas no terminal
        if (my_rank == 0) {
            printf("[P0] Cons %d | %-16s -> (%d, %d, %d) [Prod %d]\n", 
                   id, clock.acao, clock.p[0], clock.p[1], clock.p[2], clock.prod_id);
        } else if (my_rank == 1) {
            printf("\t\t\t\t[P1] Cons %d | %-16s -> (%d, %d, %d) [Prod %d]\n", 
                   id, clock.acao, clock.p[0], clock.p[1], clock.p[2], clock.prod_id);
        } else {
            printf("\t\t\t\t\t\t\t\t[P2] Cons %d | %-16s -> (%d, %d, %d) [Prod %d]\n", 
                   id, clock.acao, clock.p[0], clock.p[1], clock.p[2], clock.prod_id);
        }
        fflush(stdout); // Força a impressão imediata para não embaralhar

        if (CENARIO == 1) sleep(2); else sleep(1);
    }
    return NULL;
}

int main(int argc, char* argv[]) {
    int provided;
    
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);

    pthread_mutex_init(&mutex_fila, NULL);
    pthread_mutex_init(&mutex_relogio, NULL); // Inicializa o novo mutex do relógio
    pthread_cond_init(&condEmpty, NULL);
    pthread_cond_init(&condFull, NULL);

    pthread_t produtores[NUM_PRODUTORES];
    pthread_t consumidores[NUM_CONSUMIDORES];
    int ids[3] = {0, 1, 2};

    if (my_rank == 0) {
        printf("\n--- INICIANDO EXECUÇÃO: CENÁRIO %d ---\n\n", CENARIO);
    }
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

    pthread_mutex_destroy(&mutex_fila);
    pthread_mutex_destroy(&mutex_relogio);
    pthread_cond_destroy(&condEmpty);
    pthread_cond_destroy(&condFull);
    
    MPI_Finalize();
    return 0;
}