

def display_join_health(verification_results, view, session):
    """Display join health status"""
    combined_metrics = verification_results['combined_overlap']
    selected_join = session.get('selected_join')

    # Create field keys for verification results
    customer_key = f"{selected_join['customer_mapping']['table1_field']}_{selected_join['customer_mapping']['table2_field']}"
    date_key = f"{selected_join['date_mapping']['table1_field']}_{selected_join['date_mapping']['table2_field']}"

    # Create a comprehensive message combining both recommendation and health metrics
    message = (
        f"🎯 **Selected Join Strategy:**\n\n"  # Added extra newline for spacing
        f"   • Customer: {selected_join['customer_mapping']['table1_field']} ↔ {selected_join['customer_mapping']['table2_field']}"
        f" (Match Rate: {verification_results[customer_key]['overlap_percentage']:.1f}%)\n\n"
        f"   • Date: {selected_join['date_mapping']['table1_field']} ↔ {selected_join['date_mapping']['table2_field']}"
        f" (Match Rate: {verification_results[date_key]['overlap_percentage']:.1f}%)\n\n"
    )

    # Add product mapping if it exists
    if 'product_mapping' in selected_join:
        product_key = f"{selected_join['product_mapping']['table1_field']}_{selected_join['product_mapping']['table2_field']}"
        message += (
            f"\n   • Product: {selected_join['product_mapping']['table1_field']} ↔ {selected_join['product_mapping']['table2_field']}"
            f" (Match Rate: {verification_results[product_key]['overlap_percentage']:.1f}%)"
        )

    message += (
        f"\n\n📊 **Overall Join Impact:**\n\n"  # Added extra newline for spacing
        f"   • Total Records: {combined_metrics['total_records_table1']:,} (Table 1) ↔ {combined_metrics['total_records_table2']:,} (Table 2)\n\n"
        f"   • Matching Records: {combined_metrics['matching_records']:,}\n\n"
        f"   • Overall Match Rate: {combined_metrics['overlap_percentage']:.1f}%"
    )
    if session.get('join_confirmed'):
        view.show_message(message, "success")
    else:
        view.show_message(message, "info") 