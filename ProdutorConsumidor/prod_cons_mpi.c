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
#define NUM_PRODUTORES 1
#define NUM_CONSUMIDORES 1

// 1 = Fila Cheia (Produtores rápidos) | 2 = Fila Vazia (Consumidores rápidos)
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
Clock relogio_processo = {{0, 0, 0}, -1, "Init"};
pthread_mutex_t mutex_relogio;

int my_rank;
int comm_sz;

/* --- DELAYS DOS CENÁRIOS --- */
void delay_prod() { if (CENARIO == 1) sleep(1); else sleep(2); }
void delay_cons() { if (CENARIO == 1) sleep(2); else sleep(1); }

/* --- FUNÇÕES DA FILA --- */
Clock getClock(int id) {
    pthread_mutex_lock(&mutex_fila);
    while (count == 0) {
        printf(">>> [P%d] Fila VAZIA! Consumidor bloqueado.\n", my_rank);
        pthread_cond_wait(&condEmpty, &mutex_fila);
    }
    Clock clock = queue[0];
    for (int i = 0; i < count - 1; i++) queue[i] = queue[i+1];
    count--;
    pthread_mutex_unlock(&mutex_fila);
    pthread_cond_signal(&condFull);
    return clock;
}

void submitClock(Clock clock, int id) {
    pthread_mutex_lock(&mutex_fila);
    while (count == BUFFER_SIZE) {
        printf(">>> [P%d] Fila CHEIA! Produtor bloqueado.\n", my_rank);
        pthread_cond_wait(&condFull, &mutex_fila);
    }
    queue[count] = clock;
    count++;
    pthread_mutex_unlock(&mutex_fila);
    pthread_cond_signal(&condEmpty);
}

/* --- FUNÇÕES DO RELÓGIO VETORIAL --- */

void Event(int id_thread, const char* label) {
    pthread_mutex_lock(&mutex_relogio);
    relogio_processo.p[my_rank]++; 
    relogio_processo.prod_id = id_thread;
    sprintf(relogio_processo.acao, "Evento %s", label);
    
    Clock copia = relogio_processo; 
    pthread_mutex_unlock(&mutex_relogio);
    submitClock(copia, id_thread);
}

void Send(int dest, int id_thread, const char* label) {
    pthread_mutex_lock(&mutex_relogio);
    relogio_processo.p[my_rank]++;
    relogio_processo.prod_id = id_thread;
    sprintf(relogio_processo.acao, "Send %s -> P%d", label, dest);
    
    int buffer_envio[3] = {relogio_processo.p[0], relogio_processo.p[1], relogio_processo.p[2]};
    Clock copia = relogio_processo;
    pthread_mutex_unlock(&mutex_relogio);
    
    MPI_Send(buffer_envio, 3, MPI_INT, dest, 0, MPI_COMM_WORLD);
    submitClock(copia, id_thread);
}

void Receive(int src, int id_thread, const char* label) {
    int recebe[3];
    MPI_Recv(recebe, 3, MPI_INT, src, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    
    pthread_mutex_lock(&mutex_relogio);
    relogio_processo.p[my_rank]++;
    for (int i = 0; i < 3; i++) {
        if (relogio_processo.p[i] < recebe[i]) relogio_processo.p[i] = recebe[i];
    }
    relogio_processo.prod_id = id_thread;
    sprintf(relogio_processo.acao, "Recv %s <- P%d", label, src);
    
    Clock copia = relogio_processo;
    pthread_mutex_unlock(&mutex_relogio);
    submitClock(copia, id_thread);
}

/* --- THREADS PRODUTORAS E CONSUMIDORAS --- */

void* producerThread(void* arg) {
    int id = *(int*)arg;

    // Executa a linha do tempo exata da imagem APENAS UMA VEZ
    if (my_rank == 0) {
        Event(id, "a");         delay_prod();
        Send(1, id, "b");       delay_prod();
        Receive(1, id, "c");    delay_prod();
        Send(2, id, "d");       delay_prod();
        Receive(2, id, "e");    delay_prod();
        Send(1, id, "f");       delay_prod();
        Event(id, "g");         delay_prod();
    } 
    else if (my_rank == 1) {
        Send(0, id, "h");       delay_prod();
        Receive(0, id, "i");    delay_prod();
        Receive(0, id, "j");    delay_prod();
    } 
    else if (my_rank == 2) {
        Event(id, "k");         delay_prod();
        Send(0, id, "l");       delay_prod();
        Receive(0, id, "m");    delay_prod();
    }
    
    return NULL; // Fim da produção
}

void* consumerThread(void* arg) {
    // Define exatamente quantos eventos este processo vai gerar e consumir
    int total_eventos = 0;
    if (my_rank == 0) total_eventos = 7;
    if (my_rank == 1) total_eventos = 3;
    if (my_rank == 2) total_eventos = 3;

    // Consome exatamente o número de vezes estipulado e para
    for (int i = 0; i < total_eventos; i++) {
        Clock clock = getClock(0);

        if (my_rank == 0) {
            printf("[P0] | %-16s -> (%d, %d, %d)\n", clock.acao, clock.p[0], clock.p[1], clock.p[2]);
        } else if (my_rank == 1) {
            printf("\t\t\t\t[P1] | %-16s -> (%d, %d, %d)\n", clock.acao, clock.p[0], clock.p[1], clock.p[2]);
        } else {
            printf("\t\t\t\t\t\t\t\t[P2] | %-16s -> (%d, %d, %d)\n", clock.acao, clock.p[0], clock.p[1], clock.p[2]);
        }
        fflush(stdout); 
        delay_cons();
    }
    
    return NULL; // Fim do consumo
}

int main(int argc, char* argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);

    pthread_mutex_init(&mutex_fila, NULL);
    pthread_mutex_init(&mutex_relogio, NULL); 
    pthread_cond_init(&condEmpty, NULL);
    pthread_cond_init(&condFull, NULL);

    pthread_t produtores[NUM_PRODUTORES];
    pthread_t consumidores[NUM_CONSUMIDORES];
    int ids[1] = {0};

    if (my_rank == 0) {
        printf("\n--- INICIANDO DIAGRAMA EXATO : CENÁRIO %d ---\n\n", CENARIO);
    }
    sleep(1);

    for (int i = 0; i < NUM_PRODUTORES; i++) {
        pthread_create(&produtores[i], NULL, producerThread, &ids[i]);
    }
    for (int i = 0; i < NUM_CONSUMIDORES; i++) {
        pthread_create(&consumidores[i], NULL, consumerThread, &ids[i]);
    }

    // Agora o programa principal ficará bloqueado aqui apenas até a 
    // produção e consumo exatos terminarem, e depois seguirá para o fim.
    for (int i = 0; i < NUM_PRODUTORES; i++) pthread_join(produtores[i], NULL);
    for (int i = 0; i < NUM_CONSUMIDORES; i++) pthread_join(consumidores[i], NULL);

    // Sincroniza todos os processos MPI para o terminal não ficar bagunçado antes de fechar
    MPI_Barrier(MPI_COMM_WORLD);

    if (my_rank == 0) {
        printf("\n--- EXECUÇÃO FINALIZADA COM SUCESSO ---\n\n");
    }

    pthread_mutex_destroy(&mutex_fila);
    pthread_mutex_destroy(&mutex_relogio);
    pthread_cond_destroy(&condEmpty);
    pthread_cond_destroy(&condFull);
    
    MPI_Finalize();
    return 0;
}
