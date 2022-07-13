--TPC-DS Q41
select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 761 and 761+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'puff' or i_color = 'saddle') and 
        (i_units = 'Bundle' or i_units = 'Cup') and
        (i_size = 'extra large' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'sienna' or i_color = 'firebrick') and
        (i_units = 'Bunch' or i_units = 'N/A') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'light' or i_color = 'frosted') and
        (i_units = 'Ton' or i_units = 'Pound') and
        (i_size = 'economy' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'brown' or i_color = 'peru') and
        (i_units = 'Dozen' or i_units = 'Pallet') and
        (i_size = 'extra large' or i_size = 'small')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'coral' or i_color = 'lemon') and 
        (i_units = 'Lb' or i_units = 'Box') and
        (i_size = 'extra large' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'khaki' or i_color = 'orange') and
        (i_units = 'Gram' or i_units = 'Unknown') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'navy' or i_color = 'spring') and
        (i_units = 'Ounce' or i_units = 'Tbl') and
        (i_size = 'economy' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'royal' or i_color = 'lavender') and
        (i_units = 'Each' or i_units = 'Case') and
        (i_size = 'extra large' or i_size = 'small')
        )))) > 0
 order by i_product_name
 limit 100;

