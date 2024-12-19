from sfn_blueprint.views.streamlit_view import SFNStreamlitView
from typing import List, Optional
import streamlit as st
from typing import Any

class StreamlitView(SFNStreamlitView):
    @property
    def session_state(self):
        """Access to Streamlit's session state"""
        return st.session_state
    
    def select_box(self, label: str, options: List[str], key: Optional[str] = None) -> str:
        return st.selectbox(label, options, key=key)
    
    def file_uploader(self, label: str, key: str, accepted_types: List[str], disabled: bool = False) -> Optional[str]:
        """Display a file uploader with disabled state support"""
        return st.file_uploader(label, key=key, type=accepted_types, disabled=disabled)

    def display_button(self, label: str, key: Optional[str] = None, use_container_width: bool = False) -> bool:
        """Display a button with proper labeling"""
        button_key = key if key else f"button_{label}"
        return st.button(label=label, key=button_key, use_container_width=use_container_width)
        
    def radio_select(self, label: str, options: List[str], key: Optional[str] = None, index: Optional[int] = None) -> str:
        """Override radio button to support None as default selection"""
        return st.radio(label, options, key=key, index=index)
