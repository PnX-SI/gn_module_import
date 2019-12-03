import { Injectable } from '@angular/core';
import { FormGroup } from '@angular/forms';
import { DataService } from '../data.service';
import { ToastrService } from 'ngx-toastr';


@Injectable()
export class ContentMappingService {

    public contentMappingForm: FormGroup;
    public userContentMappings;
	public newMapping: boolean = false;
	public id_mapping;


    constructor(
        private _ds: DataService, 
        private toastr: ToastrService
    ) {}


    getMappingNamesList(mapping_type, importId) {
        // get list of existing content mapping in the select
		this._ds.getMappings(mapping_type, importId).subscribe(
			(result) => {
                console.log(result);
                this.userContentMappings = result['mappings'];
			},
			(error) => {
				console.log(error);
				if (error.statusText === 'Unknown Error') {
					// show error message if no connexion
					this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
				} else {
					console.log(error);
                    this.toastr.error(error.error.message);
				}
			}
		);
    }



    
}