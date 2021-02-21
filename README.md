# DesafioInfoPrice

## Table of contents
* [Informações gerais](#Informações-gerais)
* [Arquitetura do projeto](#arquitetura-do-projeto)
* [Tecnologias](#tecnologias)
* [Como utilizar o projeto](#como-utilizar-o-projeto)

## Informações gerais
Neste projeto, haviam dois desafios, um de desenvolver um ETL dos arquivos e o outro de fazer um web scrapping de alguma pagina, na qual foi escolido a da americanas

## Arquitetura do projeto
Neste desafio foi separado em: data onde deverá conter os dados de input e output do projeto, src onde contém o código dos dois desafios, separados pela pasta Gtin_Project e Scrap_project para dividir cada projeto em cada pasta, com um arquivo main que chama os dois.

## Tecnologias
Project is created with:
* Python 3.8
* pandas
* Selenium
* Pyspark

## Como utilizar o projeto
Este projeto utiliza o spark local, armazenando os dados no próprio disco, sendo assim, deve se instalar a ultima versão do spark na máquina, para utilizar o web scrapping, deverá ter instalado na máquina a ultima versão do chrome, para que o selenium funcione
