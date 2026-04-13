# Pipeline de Integração: Prontuário Eletrônico ↔ REDCap

![Status do Projeto](https://img.shields.io/badge/Status-Finalizado-brightgreen)
![Linguagem](https://img.shields.io/badge/Python-3.10%2B-blue)
![Área](https://img.shields.io/badge/Engenharia_de_Dados-Saúde-red)

## 📋 Descrição do Projeto
Este repositório contém o código-fonte e a documentação do Trabalho de Conclusão de Curso (TCC) em Ciência de Dados (UNIVESP 2026). 
O projeto implementa um pipeline de dados automatizado para extrair, transformar e carregar (ETL) registros clínicos de cirurgia valvar 
vindos de um prontuário eletrônico institucional para a plataforma de pesquisa REDCap.

A solução foca na **Governança de Dados** e na **Qualidade de Software**, mitigando o risco de perda de conhecimento e eliminando processos 
manuais suscetíveis a erros.

## 🏗️ Arquitetura do Sistema
O pipeline segue a estrutura clássica de ETL, dividida em módulos para garantir a manutenibilidade:

1.  **Extração:** Queries SQL otimizadas para sistemas legados.
2.  **Transformação:** Limpeza de dados, padronização de unidades (IMC, Creatinina), calculadoras clínicas (EuroScore II, Cockcroft-Gault) e tratamento de duplicidades.
3.  **Carga:** Integração via API do REDCap com lógica de preservação de dados (não sobrescreve entradas manuais da equipe médica).

## 📊 Auditoria de Software (Qualidade do Código)
O projeto foi submetido a testes de métricas estáticas para garantir a sustentabilidade hospitalar:

| Métrica | Resultado | Impacto |
| :--- | :--- | :--- |
| **Complexidade Ciclomática (McCabe)** | 5.06 (Nota B) | Código modular e de fácil manutenção. |
| **Índice de Manutenibilidade (MI)** | Evolução para nota C/A | Redução severa de débito técnico. |
| **Estimativa de Bugs (Halstead)** | 7.66 (Em melhoria) | Tendência de queda conforme modularização. |
| **Princípio DRY** | 100% aplicado | Lógica de IMC e cálculos centralizada. |

## 🚀 Tecnologias Utilizadas
* **Python:** Linguagem core do projeto.
* **Pandas:** Manipulação e transformação de grandes volumes de dados.
* **API REDCap (PyCap):** Interface de comunicação segura.
* **SQLAlchemy:** Abstração de banco de dados.
* **Matplotlib/Seaborn:** Geração de indicadores de volumetria.

## 📁 Estrutura do Repositório
* `/scripts`: Scripts Python do pipeline e relatórios de auditoria.
* `/dados/sinteticos`: Dados sintéticos para conhecimento e uso em testes.
* `/docs`: Modelos dos instrumentos no REDCap, dicionário de dados e documentos com html/css.
* `/images`: Imagens e vídeos para a composição do trabalho e da apresentação.

## 🧪 Como Verificar
1. Clone o repositório: `git clone https://github.com/priskaurdi/UNIVESP_BCD_TCC530_A2026S1N3_GP6.git`

## 👨‍💻 Autores
* Priscilla A. Nascimento
* Adriana G. Correa
* Armando Romio Junior
* Camila C. D. de Carvalho
* Demetrio O. Rumi
* Guilherme L. Parreiras
* Leonel A. L. Fernandez
* Nelson A. H. Gomez

**Orientador:** Caio dos Santos Machado  
**Instituição:** UNIVESP (2026)
