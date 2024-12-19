# Data Join Advisor

An AI-powered data joining tool that analyzes your datasets and provides intelligent suggestions for joining tables, with interactive validation and health checks.

🌟 Features
- **Intelligent Join Analysis**: Automatically analyzes potential join fields between tables

- **Smart Join Suggestions**: Generates contextual join recommendations based on:
    - Customer ID fields
    - Date fields
    - Product ID fields (optional)

- **Join Health Validation**: Comprehensive join quality metrics including:
    - Match rates
    - Date range analysis
    - Value overlap statistics

- **Flexible Data Input**: Supports multiple file formats (CSV, Excel, JSON, Parquet)

- **Interactive Join Configuration**: Choose between AI recommendations or manual column mapping

- **Data Export**: Download joined data in CSV format

🚀 Getting Started
**Prerequisites**
- Python 3.8+
- OpenAI API key

Installation

1. Clone the repository:
```
git clone [repository-url]
cd [repository-name]
```

2.Create and activate a virtual environment:
```
python -m venv myenv
source myenv/bin/activate # Linux/Mac
.\myenv\Scripts\activate # Windows

```

3. Install dependencies:
```
pip install -r requirements.txt
```

4. Set up your OpenAI API key:
```
export OPENAI_API_KEY='your-api-key'
```

5. Running the Application
```
streamlit run app.py
```

🔄 Workflow
1. **Data Loading**
    - Upload two datasets to join (CSV, Excel, JSON, or Parquet)
    - Preview both datasets

2. **Join Analysis & Suggestions**
    - AI analyzes potential join fields between tables
    - Generates join suggestions based on:
        - Customer ID field matches
        - Date field alignments
        - Optional Product ID matches

3. **Join Strategy Selection**
    - Choose between:
        - Using AI recommended join strategy
        - Manual column mapping
    - Validate join health with detailed metrics

4. **Post Processing**
    - View joined data preview
    - Download joined dataset
    - Review join summary


🛠️ Architecture
The application follows a modular architecture with these key components:

- **SFNJoinSuggestionsAgent**: Analyzes tables and generates join suggestions

- **SFNDataLoader**: Handles data import and initial processing

- **SFNDataPostProcessor**: Manages data export and final processing

- **StreamlitView**: Manages the user interface

- **SFNSessionManager**: Handles application state

📊 Join Analysis Features
The tool analyzes multiple aspects of your data joins:
- **Field compatibility analysis**
- **Date range overlap**
- **Value match rates**
- **Join impact assessment**
- **Combined field verification**

🔒 Security
- Secure data handling
- Input validation
- Environment variables for sensitive data
- Safe join operations

📝 License
MIT License

🤝 Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (git checkout -b feature/AmazingFeature)
3. Commit your changes (git commit -m 'Add some AmazingFeature')
Push to the branch (git push origin feature/AmazingFeature)
4. Open a Pull Request

📧 Contact
Email: puneet@stepfunction.ai