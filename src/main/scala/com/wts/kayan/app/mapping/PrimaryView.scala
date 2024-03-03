package com.wts.kayan.app.mapping

object PrimaryView {

  val get_SqlString: String =
    """
      |select uv.*,
      |       cast(null as string) code_cee_hcee_priv,
      |	      cast(null as string) source_appl_priv,
      |	      cast(null as string) prod_typ_bo_sg_priv,
      |	      cast(null as string) code_cat_fisc_priv,
      |	      cast(null as string) event_nature_priv,
      |
      |	      'Warning' as control_result,
      |
      |	      current_date as control_date,
      |       'control_base_type_contrepartie' as control_name
      |from uv_prorata uv
      |where uv.type_contrepartie is null
      |""".stripMargin

}
