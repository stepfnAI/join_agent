import sys
import os
from sfn_blueprint import Task
from sfn_blueprint import SFNStreamlitView
from sfn_blueprint import SFNSessionManager
from sfn_blueprint import SFNDataLoader
from sfn_blueprint import setup_logger
from sfn_blueprint import SFNDataPostProcessor
from agents.join_suggestions_agent import SFNJoinSuggestionsAgent
from views.streamlit_views import StreamlitView
import json

def run_app():
    # Initialize view and session
    view = StreamlitView(title="Data Join Advisor")
    session = SFNSessionManager()
    
    # Reset button
    col1, col2 = view.create_columns([7, 1])
    with col1:
        view.display_title()
    with col2:
        if view.display_button("🔄", key="reset_button"):
            session.clear()
            view.rerun_script()

    # Setup logger
    logger, handler = setup_logger()
    logger.info('Starting Data Join Advisor')

    # Step 1: Data Loading and Preview
    view.display_header("Step 1: Data Loading")
    view.display_markdown("---")
    
    # Create two columns for file uploads
    col1, col2 = view.create_columns(2)
    
    with col1:
        view.display_subheader("Table 1")
        uploaded_file1 = view.file_uploader("Choose first file", 
                                        #   key="file1",
                                          accepted_types=["csv", "xlsx", "json", "parquet"])
        
    with col2:
        view.display_subheader("Table 2")
        uploaded_file2 = view.file_uploader("Choose second file", 
                                        #   key="file2",
                                          accepted_types=["csv", "xlsx", "json", "parquet"])

    # Load and preview data
    if uploaded_file1 is not None and uploaded_file2 is not None:
        if session.get('table1') is None or session.get('table2') is None:
            with view.display_spinner('Loading data...'):
                data_loader = SFNDataLoader()
                
                # Load table 1
                load_task1 = Task("Load first file", data=uploaded_file1)
                table1 = data_loader.execute_task(load_task1)
                session.set('table1', table1)
                
                # Load table 2
                load_task2 = Task("Load second file", data=uploaded_file2)
                table2 = data_loader.execute_task(load_task2)
                session.set('table2', table2)
                
                logger.info(f"Data loaded successfully. Table1 shape: {table1.shape}, Table2 shape: {table2.shape}")
                view.show_message("✅ Both tables loaded successfully!", "success")

        # Display data previews
        view.display_subheader("Data Preview")
        col1, col2 = view.create_columns(2)
        with col1:
            view.show_message("Table 1 Preview", "info")
            view.display_dataframe(session.get('table1').head())
        with col2:
            view.show_message("Table 2 Preview", "info")
            view.display_dataframe(session.get('table2').head())

        # Step 2: Generate Join Suggestions
        view.display_header("Step 2: Join Analysis")
        view.display_markdown("---")

        if session.get('join_analysis') is None:
            # Initial Analysis
            with view.display_spinner('🤖 AI is analyzing possible join combinations...'):
                join_analyzer = SFNJoinSuggestionsAgent()
                analysis_task = Task("Analyze join possibilities", 
                                data={'table1': session.get('table1'),
                                        'table2': session.get('table2')})
                join_analysis = join_analyzer.execute_task(analysis_task)
                session.set('join_analysis', join_analysis)
                logger.info("Join analysis completed")

        if session.get('join_analysis'):
            analysis = session.get('join_analysis')
            
            # Show number of suggestions found
            suggestion_count = analysis['suggestion_count']
            if suggestion_count == 0:
                view.show_message("❌ No valid join combinations found.", "error")
                return
            elif suggestion_count == 1:
                view.show_message("✅ Found 1 possible way to join these tables.", "success")
            else:
                view.show_message(f"✅ Found {suggestion_count} possible ways to join these tables.", "success")

        
             # Display join suggestions
            view.display_subheader("Available Join Options")
            suggestion_data = analysis.get('initial_suggestions', {})

        
            if isinstance(suggestion_data, str):
                try:
                    suggestion_data = json.loads(suggestion_data)
                except json.JSONDecodeError:
                    view.show_message("❌ Error parsing suggestions", "error")
                    return

            for suggestion_key, suggestion in suggestion_data.items():
                # Create verification result keys based on the mappings
                date_key = f"{suggestion.get('DateField', {}).get('table1')}_{suggestion.get('DateField', {}).get('table2')}"
                customer_key = f"{suggestion.get('CustIDField', {}).get('table1')}_{suggestion.get('CustIDField', {}).get('table2')}"
                
                # Get metrics from verification results
                date_metrics = analysis['verification_results'].get(date_key, {})
                customer_metrics = analysis['verification_results'].get(customer_key, {})
                
                view.show_message(
                    f"Option {suggestion_key}:\n"
                    f"📌 Join Fields:\n"
                    f"- Date: {suggestion.get('DateField', {}).get('table1')} ↔ {suggestion.get('DateField', {}).get('table2')}\n"
                    f"- Customer: {suggestion.get('CustIDField', {}).get('table1')} ↔ {suggestion.get('CustIDField', {}).get('table2')}"
                    + (f"\n- Product: {suggestion.get('ProdID', {}).get('table1')} ↔ {suggestion.get('ProdID', {}).get('table2')}" 
                    if suggestion.get('ProdID', {}).get('table1') else ""),
                    "info"
                )
                
                view.display_markdown("---")

            # Display AI's recommendation
            view.display_subheader("AI Recommended Join Strategy")
            recommendation = analysis['final_recommendations']
            
            # Only show AI recommendations and join selection if join is not confirmed
            if not session.get('join_confirmed'):
                if 'recommended_join' in recommendation:  # Check for the nested structure
                    recommended_join = recommendation['recommended_join']  # Get the nested object
                    view.display_subheader("Final Recommendation")
                    
                    # Safely construct the message
                    message = (
                        f"🎯 Recommended Join Fields:\n"
                        f"- Date: {recommended_join.get('date_mapping', {}).get('table1_field', 'N/A')} ↔ {recommended_join.get('date_mapping', {}).get('table2_field', 'N/A')}\n"
                        f"- Customer: {recommended_join.get('customer_mapping', {}).get('table1_field', 'N/A')} ↔ {recommended_join.get('customer_mapping', {}).get('table2_field', 'N/A')}"
                    )
                    
                    # Only add product mapping info if it exists and has valid fields
                    if (recommended_join.get('product_mapping') and 
                        recommended_join.get('product_mapping', {}).get('table1_field') and 
                        recommended_join.get('product_mapping', {}).get('table2_field')):
                        message += f"\n- Product: {recommended_join['product_mapping']['table1_field']} ↔ {recommended_join['product_mapping']['table2_field']}"
                    
                    view.show_message(message, "success")
                    
                    # Only show explanation if it exists
                    if 'explanation' in recommended_join:
                        view.show_message(f"📝 Reasoning:\n{recommended_join['explanation']}", "info")
                    else:
                        view.show_message("📝 No detailed explanation available for this recommendation.", "info")
                else:
                    view.show_message("❌ No recommendation available.", "error")

                join_choice = view.radio_select(
                    "How would you like to proceed?",
                    options=[
                        "Select Appropriate Method",
                        "Use AI Recommended Join Strategy",
                        "Select Columns Manually"
                    ]
                )

                # Show AI recommendation details and health check option
                if join_choice == "Use AI Recommended Join Strategy":
                    selected_join = {
                        'customer_mapping': recommendation['recommended_join']['customer_mapping'],
                        'date_mapping': recommendation['recommended_join']['date_mapping']
                    }
                    if ('product_mapping' in recommendation['recommended_join'] and 
                        recommendation['recommended_join'].get('product_mapping', {}).get('table1_field') and 
                        recommendation['recommended_join'].get('product_mapping', {}).get('table2_field')):
                        selected_join['product_mapping'] = recommendation['recommended_join']['product_mapping']
                    
                    # Store selected_join in session immediately
                    session.set('selected_join', selected_join)
                    
                    # Add health check button for AI recommendation
                    if view.display_button("Check AI Recommendation Join Health", key='check_ai_health'):
                        table1 = session.get('table1')
                        table2 = session.get('table2')
                        health_check = check_join_health(table1, table2, selected_join)
                        display_join_health(health_check, view, session)
                        session.set('join_health', health_check)  
                
                if join_choice == "Select Columns Manually":
                    view.display_subheader("Manual Column Mapping")
                    
                    # Get tables from session
                    table1 = session.get('table1')
                    table2 = session.get('table2')
                    
                    # Create columns for side-by-side selection
                    col1, col2 = view.create_columns(2)
                    
                    with col1:
                        view.show_message("Table 1 Columns", "info")
                        table1_cols = [''] + list(table1.columns)
                        cust_id_col1 = view.select_box(
                            "Customer ID Column (Required)",
                            options=table1_cols,
                            key="cust_id_1"
                        )
                        date_col1 = view.select_box(
                            "Date Column (Required)",
                            options=table1_cols,
                            key="date_1"
                        )
                        prod_col1 = view.select_box(
                            "Product Column (Optional)",
                            options=['None'] + list(table1.columns),
                            key="prod_1"
                        )
                    
                    with col2:
                        view.show_message("Table 2 Columns", "info")
                        table2_cols = [''] + list(table2.columns)
                        cust_id_col2 = view.select_box(
                            "Customer ID Column (Required)",
                            options=table2_cols,
                            key="cust_id_2"
                        )
                        date_col2 = view.select_box(
                            "Date Column (Required)",
                            options=table2_cols,
                            key="date_2"
                        )
                        prod_col2 = view.select_box(
                            "Product Column (Optional)",
                            options=['None'] + list(table2.columns),
                            key="prod_2"
                        )

                    # Create mapping configuration
                    selected_join = {
                        'customer_mapping': {
                            'table1_field': cust_id_col1,
                            'table2_field': cust_id_col2
                        },
                        'date_mapping': {
                            'table1_field': date_col1,
                            'table2_field': date_col2
                        }
                    }
                    
                    # Add product mapping only if both product columns are selected and not 'None'
                    if prod_col1 != 'None' and prod_col2 != 'None':
                        selected_join['product_mapping'] = {
                            'table1_field': prod_col1,
                            'table2_field': prod_col2
                        }
                    
                    # Validate selections
                    is_valid = (
                        cust_id_col1 and cust_id_col2 and  # Customer ID fields selected
                        date_col1 and date_col2 and        # Date fields selected
                        cust_id_col1 != '' and cust_id_col2 != '' and  # Not empty strings
                        date_col1 != '' and date_col2 != ''            # Not empty strings
                    )
                    
                    if not is_valid:
                        view.show_message("⚠️ Please select required mapping fields (Customer ID and Date)", "warning")
                    else:
                        # Store selected_join in session
                        session.set('selected_join', selected_join)
                        
                        # Show button to check join health
                        if view.display_button("Check Join Health", key='check_health'):
                            health_check = check_join_health(table1, table2, selected_join)
                            display_join_health(health_check, view, session)
                            session.set('join_health', health_check)


            # Add confirmation button here, outside of display_join_health
            if session.get('join_health') and not session.get('join_confirmed'):
                if view.display_button("Confirm Join Strategy", key="confirm_join"):
                    session.set('join_confirmed', True)
                    view.show_message("✅ Join strategy confirmed! Proceed to next step.", "success")
                    view.rerun_script()    

            if session.get('join_confirmed'):
                # Show join summary
                table1 = session.get('table1')
                table2 = session.get('table2')
                selected_join = session.get('selected_join')
                
                # Count mapped fields
                mapped_fields = sum(1 for k in ['customer_mapping', 'date_mapping', 'product_mapping'] 
                                if k in selected_join and selected_join[k].get('table1_field'))
                
                display_join_health(session.get('join_health'), view, session)

                view.display_header("Step 3: Post Processing")
                view.display_markdown("---")
                
                operation_type = view.radio_select(
                    "Choose an operation:",
                    ["View Joined Data", "Download Joined Data", "Finish"]
                )

                # Perform join operation if not already done
                if session.get('final_df') is None:
                    with view.display_spinner('Joining tables...'):
                        # Create merge conditions
                        merge_conditions = [
                            (selected_join['customer_mapping']['table1_field'], 
                            selected_join['customer_mapping']['table2_field']),
                            (selected_join['date_mapping']['table1_field'], 
                            selected_join['date_mapping']['table2_field'])
                        ]
                        
                        # Add product mapping if exists
                        if ('product_mapping' in selected_join and 
                            selected_join['product_mapping'].get('table1_field')):
                            merge_conditions.append(
                                (selected_join['product_mapping']['table1_field'], 
                                selected_join['product_mapping']['table2_field'])
                            )
                        
                        # Perform merge
                        joined_df = table1.merge(
                            table2,
                            left_on=[m[0] for m in merge_conditions],
                            right_on=[m[1] for m in merge_conditions],
                            how='inner'
                        )
                        session.set('final_df', joined_df)

                if operation_type == "View Joined Data":
                    view.display_subheader("Joined Data Preview")
                    view.display_dataframe(session.get('final_df').head(10))
                
                elif operation_type == "Download Joined Data":
                    post_processor = SFNDataPostProcessor(session.get('final_df'))
                    csv_data = post_processor.download_data('csv')
                    view.create_download_button(
                        label="Download CSV",
                        data=csv_data,
                        file_name="joined_data.csv",
                        mime_type="text/csv"
                    )
                
                elif operation_type == "Finish":
                    if view.display_button("Confirm Finish"):
                        view.show_message("Thank you for using the Data Join Advisor!", "success")
                        session.clear()
                        view.rerun_script()


def check_join_health(table1, table2, selected_join):
    """Check join health for manually selected columns"""
    verification_results = {}
    
    # Check individual field overlaps
    for mapping_type, mapping in selected_join.items():
        field1 = mapping['table1_field']
        field2 = mapping['table2_field']
        
        values1 = set(table1[field1].dropna().unique())
        values2 = set(table2[field2].dropna().unique())
        overlap = values1.intersection(values2)
        
        verification_results[f"{field1}_{field2}"] = {
            "overlap_percentage": len(overlap) / max(len(values1), len(values2)) * 100,
            "total_values_table1": len(values1),
            "total_values_table2": len(values2),
            "overlapping_values": len(overlap)
        }
    
    # Check combined overlap
    merge_conditions = [
        (selected_join['customer_mapping']['table1_field'], 
         selected_join['customer_mapping']['table2_field']),
        (selected_join['date_mapping']['table1_field'], 
         selected_join['date_mapping']['table2_field'])
    ]
    
    if 'product_mapping' in selected_join:
        merge_conditions.append(
            (selected_join['product_mapping']['table1_field'], 
             selected_join['product_mapping']['table2_field'])
        )
    
    merged_df = table1.merge(
        table2,
        left_on=[m[0] for m in merge_conditions],
        right_on=[m[1] for m in merge_conditions],
        how='inner'
    )
    
    verification_results['combined_overlap'] = {
        "total_records_table1": len(table1),
        "total_records_table2": len(table2),
        "matching_records": len(merged_df),
        "overlap_percentage": (len(merged_df) / min(len(table1), len(table2))) * 100
    }
    
    return verification_results


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

    view.show_message(message, "info")

if __name__ == "__main__":        
    run_app()