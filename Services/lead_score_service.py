from flask import Blueprint, jsonify, request

lead = Blueprint('leadScore', __name__)


@lead.route('/leadScore', methods=['POST'])
def lead_score():
    new_lead = request.json
    #score=model.getLeadScore(new_lead)
    #send_to_allocate(new_lead,score)
    return jsonify("score generted"), 201
