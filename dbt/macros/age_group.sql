 {#
    This macro returns day time of crash damage 
#}

{% macro age_group(age) -%}
   case
    when  {{age}} is null  then null
    when  {{age}}  < 0 then null
    when  {{age}}  < 13 then 'child' 
    when {{age}}  < 20 then 'teen'
    when  {{age}}  < 40 then 'young adult' 
    when {{age}}  < 65 then 'old adult'
    when {{age}}  >= 65 then 'eldery'
   end
{%- endmacro %}

              