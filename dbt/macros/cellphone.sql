 {#
    This macro returns day time of crash damage 
#}

{% macro cellphone(c) -%}
   case
    when  {{c}} = 'Y' then True
    when {{c}}  = 'N' then False
   end
{%- endmacro %}

              