with
    stg_categorias as (
        select *
        from {{ ref('stg_erp_categorias') }}
    )

    , stg_fornecedores as (
        select *
        from {{ ref('stg_erp_fornecedores') }}
    )

    , stg_produtos as (
        select *
        from {{ ref('stg_erp_produtos') }}
    )

    , join_tabelas as (
        select
            p.id_produto
            , p.id_fornecedor
            , p.id_categoria
            , p.nome_produto
            , p.quantidade_por_unidade
            , p.preco_por_unidade
            , p.unidades_em_estoque
            , p.unidades_por_ordem
            , p.nivel_reabastecimento
            , p.is_discontinuado
            , c.nome_categoria
            , c.descricao
            , f.nome_fornecedor
            , f.contato_fornecedor
            , f.contato_funcao
            , f.endereco_fornecedor
            , f.cidade_fornecedor
            , f.regiao_fornecedor
            , f.pais_fornecedor
            , f.cep_fornecedor
        from stg_produtos p
        left join stg_categorias c on 
            p.id_categoria = c.id_categoria
        left join stg_fornecedores f on 
            p.id_fornecedor = f.id_fornecedor
    )

    , criar_chaves as (
        select
            row_number() over (order by id_produto) as pk_produto
            , *
        from join_tabelas
    )

select *
from criar_chaves
