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
                
                # # Display metrics using the first available metrics (date or customer)
                # metrics_to_use = date_metrics if date_metrics else customer_metrics
                # if metrics_to_use:
                #     view.show_message(
                #         f"📊 Quality Metrics:\n"
                #         f"- Value Overlap: {metrics_to_use.get('value_overlap', {}).get('metrics', {}).get('overlap_percentage', 'N/A')}%\n",
                #         "info"
                #     )
                view.display_markdown("---")

            # Display AI's recommendation
            view.display_subheader("AI Recommended Join Strategy")
            recommendation = analysis['final_recommendations']
            
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

            # User decision
            join_choice = view.radio_select(
                "How would you like to proceed?",
                options=[
                    "Use AI Recommended Join Strategy",
                    "Select Columns Manually"
                ]
            )

            if join_choice == "Use AI Recommended Join Strategy":
                selected_join = {
                    'customer_mapping': recommendation['recommended_join']['customer_mapping'],
                    'date_mapping': recommendation['recommended_join']['date_mapping']
                }
                # Only add product mapping if it exists and has valid fields
                if ('product_mapping' in recommendation['recommended_join'] and 
                    recommendation['recommended_join'].get('product_mapping', {}).get('table1_field') and 
                    recommendation['recommended_join'].get('product_mapping', {}).get('table2_field')):
                    selected_join['product_mapping'] = recommendation['recommended_join']['product_mapping']
            else:  # Manual column selection
                # Create columns for side-by-side selection
                col1, col2 = view.create_columns(2)
                
                with col1:
                    view.show_message("Select columns from Table 1", "info")
                    table1_cols = session.get('table1').columns.tolist()
                    cust_id_col1 = view.select_box("Customer ID Column (Table 1)", 
                                                options=table1_cols,
                                                key="cust_id_1")
                    date_col1 = view.select_box("Date Column (Table 1)", 
                                             options=table1_cols,
                                             key="date_1")
                    prod_col1 = view.select_box("Product Column (Table 1) - Optional", 
                                             options=['None'] + table1_cols,
                                             key="prod_1")
                
                with col2:
                    view.show_message("Select columns from Table 2", "info")
                    table2_cols = session.get('table2').columns.tolist()
                    cust_id_col2 = view.select_box("Customer ID Column (Table 2)", 
                                                options=table2_cols,
                                                key="cust_id_2")
                    date_col2 = view.select_box("Date Column (Table 2)", 
                                             options=table2_cols,
                                             key="date_2")
                    prod_col2 = view.select_box("Product Column (Table 2) - Optional", 
                                             options=['None'] + table2_cols,
                                             key="prod_2")

                # Validate mandatory selections
                if not (cust_id_col1 and cust_id_col2 and date_col1 and date_col2):
                    view.show_message("⚠️ Customer ID and Date columns are mandatory!", "warning")
                    return

                # Create manual join configuration
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
                
                # Add product mapping only if both product columns are selected
                if prod_col1 != 'None' and prod_col2 != 'None':
                    selected_join['product_mapping'] = {
                        'table1_field': prod_col1,
                        'table2_field': prod_col2
                    }

            # Proceed with join
            proceed = view.display_button("Proceed with Selected Join")
            
            if proceed:
                session.set('selected_join', selected_join)
                
                # Perform the join operation
                with view.display_spinner('Joining tables...'):
                    table1 = session.get('table1')
                    table2 = session.get('table2')
                    
                    # Create merge conditions
                    merge_on = [
                        (selected_join['customer_mapping']['table1_field'], 
                         selected_join['customer_mapping']['table2_field']),
                        (selected_join['date_mapping']['table1_field'], 
                         selected_join['date_mapping']['table2_field'])
                    ]
                    
                    # Only add product mapping if it exists and has valid fields
                    if ('product_mapping' in selected_join and 
                        selected_join['product_mapping'].get('table1_field') and 
                        selected_join['product_mapping'].get('table2_field')):
                        merge_on.append(
                            (selected_join['product_mapping']['table1_field'], 
                             selected_join['product_mapping']['table2_field'])
                        )
                    
                    # Perform merge
                    joined_table = table1.merge(
                        table2,
                        left_on=[m[0] for m in merge_on],
                        right_on=[m[1] for m in merge_on],
                        how='inner'
                    )
                    
                    session.set('joined_table', joined_table)
                    
                    view.show_message("✅ Tables joined successfully!", "success")
                
                # Show options for next steps
                next_step = view.radio_select(
                    "What would you like to do next?",
                    options=[
                        "View Joined Table",
                        "Download Joined Table",
                        "Finish"
                    ]
                )
                
                if next_step == "View Joined Table":
                    view.display_subheader("Joined Table Preview")
                    view.display_dataframe(joined_table.head(10))
                    
                elif next_step == "Download Joined Table":
                    # Convert to CSV for download
                    csv = joined_table.to_csv(index=False)
                    view.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name="joined_tables.csv",
                        mime="text/csv"
                    )
                    
                elif next_step == "Finish":
                    if view.display_button("Confirm Finish"):
                        view.show_message("Thank you for using the Column Mapping App!", "success")
                        session.clear()
                        view.rerun_script()

if __name__ == "__main__":        
    run_app()