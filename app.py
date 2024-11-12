import sys
import os
from sfn_blueprint import Task
from sfn_blueprint import SFNStreamlitView
from sfn_blueprint import SFNSessionManager
from sfn_blueprint import SFNDataLoader
from sfn_blueprint import setup_logger
from sfn_blueprint import SFNDataPostProcessor
from agents.join_suggestions_agent import SFNJoinSuggestionsAgent

def run_app():
    # Initialize view and session
    view = SFNStreamlitView(title="Data Join Advisor")
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
            with view.display_spinner('🤖 AI is analyzing join possibilities...'):
                join_analyzer = SFNJoinSuggestionsAgent()
                analysis_task = Task("Analyze join possibilities", 
                                   data={'table1': session.get('table1'),
                                        'table2': session.get('table2')})
                join_analysis = join_analyzer.execute_task(analysis_task)
                session.set('join_analysis', join_analysis)
                logger.info("Join analysis completed")

        # Display Join Analysis Results
        if session.get('join_analysis'):
            analysis = session.get('join_analysis')
            
            # Display initial suggestions
            view.display_subheader("Suggested Join Fields")
            for suggestion in analysis['initial_suggestions']:
                view.show_message(
                    f"📌 Join {suggestion['table1_field']} with {suggestion['table2_field']} "
                    f"({suggestion['field_type']})\n"
                    f"Reasoning: {suggestion['reasoning']}", 
                    "info"
                )

            # Display verification results
            view.display_subheader("Join Health Analysis")
            for field_pair, results in analysis['verification_results'].items():
                view.show_message(
                    f"🔍 **{field_pair}**\n"
                    f"- Uniqueness: {results['uniqueness']['table1']:.2%} (Table 1), "
                    f"{results['uniqueness']['table2']:.2%} (Table 2)\n"
                    f"- Value Overlap: {results['overlap']['set1_coverage']:.2%}\n"
                    f"- Null Values: {results['null_percentage']['table1']:.2%} (Table 1), "
                    f"{results['null_percentage']['table2']:.2%} (Table 2)",
                    "info"
                )

            # Display final recommendations
            view.display_subheader("Final Recommendations")
            view.show_message(analysis['final_recommendations'], "success")

            # Step 3: Join Preview and Export
            view.display_header("Step 3: Join Preview and Export")
            view.display_markdown("---")

            # Allow user to select join fields and type
            selected_fields = view.multiselect(
                "Select fields to join on",
                options=[f"{s['table1_field']} ↔ {s['table2_field']}" 
                        for s in analysis['initial_suggestions']]
            )

            join_type = view.radio_select(
                "Select join type",
                options=["inner", "left", "right", "outer"]
            )

            if selected_fields and join_type:
                # Implement join preview logic here
                # For now, just show download option
                post_processor = SFNDataPostProcessor(session.get('table1'))
                csv_data = post_processor.download_data('csv')
                view.create_download_button(
                    label="Download Joined Data",
                    data=csv_data,
                    file_name="joined_data.csv",
                    mime_type="text/csv"
                )

if __name__ == "__main__":
    run_app()